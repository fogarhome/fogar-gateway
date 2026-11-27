import { NOT_IN_OPENCLAW, ownText, TURN_PROGRESS, turnErrorText } from './labels'
import { reconnectClass, type Reconnect } from './closeCodes'

// El chat del panel (docs/client-protocol.md §16): lo que llega por `/events`, cómo queda en pantalla y
// qué hacer cuando algo se corta. Sin Vue ni red: lo usa `chatSession.ts` y se prueba solo.

/** Lo que el gateway manda en una sesión de texto (`output_mode: ["text", "status"]`) que el chat usa. */
export type ServerMessage =
  | { type: 'ready'; session_id: string; agent: string; conversation?: { id: string; agent: string } }
  | {
      type: 'turn_start'
      turn_id: string
      turn_seq?: number
      conversation?: string | null
      origin?: 'user' | 'push'
      /** Quién lo inició (5.0): su aparato, y si es este; `null` en un push. */
      by?: { device_id: string; name: string; self: boolean } | null
    }
  | { type: 'token'; turn_id: string; text: string }
  | { type: 'turn_progress'; turn_id: string; phase: string; tool?: string | null }
  | { type: 'turn_end'; turn_id: string; reason?: string }
  | { type: 'error'; code: string; message: string; fatal: boolean; turn_id?: string }
  | { type: 'status'; service: string; state: string }
  | {
      type: 'message'
      id: string
      conversation: string
      text: string
      from: { source: string; device_id: string; name: string; self: boolean }
      state: MessageState
    }
  | { type: string; [key: string]: unknown }

/** El estado de un mensaje de una persona (5.0): espera en la cola, ya tiene turno, o se quitó. */
export type MessageState = 'pending' | 'delivered' | 'withdrawn'

export interface ChatLine {
  key: string
  role: 'user' | 'assistant'
  text: string
  /** ISO; `null` si no se sabe. */
  at: string | null
  /** Solo en las de Fogar: `push` es Fogar hablando sin que le preguntaran. */
  origin?: 'user' | 'push'
  /** `streaming` mientras llegan tokens; `ended` con su `turn_end` (motivo en `reason`). */
  state: 'streaming' | 'ended'
  /** En qué está la respuesta mientras no llega texto (`turn_progress`); fuera con el primer token o al terminar. */
  progress?: TurnProgress | null
  reason?: string
  /** El `message` del `error` de su turno (código estable, §9). */
  error?: string
  /** Un push de otra conversación del panel (el gateway lo entrega en la sesión más reciente). */
  otherConversation?: string | null
  /** Solo en las tuyas: escrita con la conversación dormida y no llegó a salir (falló la reconexión). */
  unsent?: boolean
  /** Solo en las de una persona (5.0): el id de su mensaje, su estado y de qué aparato es. */
  messageId?: string
  messageState?: MessageState
  from?: { name: string; self: boolean }
}

export interface TurnProgress {
  phase: string
  tool: string | null
}

/** El progreso en una frase corta: «Pensando…», «Usando ha_call_service…». Una fase nueva es «Trabajando…». */
export function progressText(progress: TurnProgress): string {
  if (progress.phase === 'tool') return progress.tool ? `Usando ${progress.tool}…` : 'Usando una herramienta…'
  return ownText(TURN_PROGRESS, progress.phase) ?? 'Trabajando…'
}

/** Las líneas de la conversación en vivo, en el orden en que llegan (el orden de `/events`). */
export class ChatLog {
  lines: ChatLine[] = []

  constructor(private conversationId: string) {}

  /**
   * Las claves de lo terminado: lo que una lectura de lo dicho pedida ahora ya trae. Un mensaje
   * pendiente o quitado no cuenta (OpenClaw aún no lo tiene, o nunca lo tendrá).
   */
  endedKeys(): string[] {
    return this.lines
      .filter((l) => (l.role === 'user' ? l.messageState === 'delivered' : l.state === 'ended'))
      .map((l) => l.key)
  }

  /**
   * Se ha vuelto a leer lo dicho de OpenClaw, que ya trae lo terminado antes de pedirlo (`keys`, de
   * `endedKeys()`): fuera eso, para no enseñarlo dos veces. Se queda lo que OpenClaw no tiene: lo de
   * otra conversación y el turno que no llegó a OpenClaw (`NOT_IN_OPENCLAW`).
   */
  dropReloaded(keys: Iterable<string>): void {
    const drop = new Set(keys)
    this.lines = this.lines.filter(
      (l) => !drop.has(l.key) || l.otherConversation || (l.error != null && NOT_IN_OPENCLAW.has(l.error)),
    )
  }

  /** Ya no queda nada en vivo (al entrar de nuevo tras un `resync`). */
  clear(): void {
    this.lines = []
  }

  private turn(turnId: string): ChatLine | undefined {
    for (let i = this.lines.length - 1; i >= 0; i--) {
      const line = this.lines[i]!
      if (line.role === 'assistant' && line.key === `t-${turnId}`) return line
    }
    return undefined
  }

  /** Aplica un evento. Devuelve `true` si cambió algo de la conversación. */
  apply(msg: ServerMessage, at = new Date().toISOString()): boolean {
    switch (msg.type) {
      case 'message': {
        const m = msg as Extract<ServerMessage, { type: 'message' }>
        if (typeof m.id !== 'string') return false
        const key = `m-${m.id}`
        const line = this.lines.find((l) => l.key === key)
        if (line) {
          line.messageState = m.state
          return true
        }
        this.lines.push({
          key,
          role: 'user',
          text: m.text,
          at,
          state: 'ended',
          reason: 'done',
          messageId: m.id,
          messageState: m.state,
          from: { name: m.from?.name ?? '', self: m.from?.self ?? false },
        })
        return true
      }
      case 'turn_start': {
        const m = msg as Extract<ServerMessage, { type: 'turn_start' }>
        if (this.turn(m.turn_id)) return false // ya estaba (lo trajo el estado al entrar)
        const other = m.conversation && m.conversation !== this.conversationId ? m.conversation : null
        this.lines.push({
          key: `t-${m.turn_id}`,
          role: 'assistant',
          text: '',
          at,
          origin: m.origin === 'push' ? 'push' : 'user',
          state: 'streaming',
          ...(other ? { otherConversation: other } : {}),
        })
        return true
      }
      case 'token': {
        const m = msg as Extract<ServerMessage, { type: 'token' }>
        const line = this.turn(m.turn_id)
        if (!line || line.state !== 'streaming') return false
        line.text += m.text
        line.progress = null // el texto ya está llegando
        return true
      }
      case 'turn_progress': {
        const m = msg as Extract<ServerMessage, { type: 'turn_progress' }>
        const line = this.turn(m.turn_id)
        if (!line || line.state !== 'streaming' || typeof m.phase !== 'string') return false
        line.progress = { phase: m.phase, tool: typeof m.tool === 'string' ? m.tool : null }
        return true
      }
      case 'error': {
        const m = msg as Extract<ServerMessage, { type: 'error' }>
        const line = m.turn_id ? this.turn(m.turn_id) : undefined
        if (!line) return false
        line.error = m.message
        return true
      }
      case 'turn_end': {
        const m = msg as Extract<ServerMessage, { type: 'turn_end' }>
        const line = this.turn(m.turn_id)
        if (!line || line.state !== 'streaming') return false
        line.state = 'ended'
        line.reason = m.reason ?? 'done'
        line.progress = null
        return true
      }
      default:
        return false
    }
  }

  /** Lo que seguía abierto ya no va a terminar aquí (su `turn_end` no llegará: `resync`). */
  closeOpenTurns(): void {
    for (const line of this.lines) {
      if (line.state === 'streaming') {
        line.state = 'ended'
        line.reason = 'closed'
        line.progress = null
      }
    }
  }

  get turnOpen(): boolean {
    return this.lines.some((line) => line.state === 'streaming')
  }

  /** El `turn_id` del turno abierto de esta conversación (no un aviso de otra que se ve aquí). */
  get openTurnId(): string | null {
    const line = this.lines.find((l) => l.role === 'assistant' && l.state === 'streaming' && !l.otherConversation)
    return line ? line.key.slice(2) : null
  }

  /** Mensajes que esperan en la cola (para «Quitar»). */
  get pending(): ChatLine[] {
    return this.lines.filter((l) => l.messageState === 'pending')
  }
}

const REASON: Record<string, string> = {
  cancelled: 'Se paró.',
  interrupted: 'Se interrumpió: se envió otro mensaje ya.',
  replaced: 'Se cortó: la sustituyó tu siguiente mensaje.',
  timeout: 'Se cortó: OpenClaw dejó de responder.',
  closed: 'Se cortó: se cerró la conexión.',
}

/** Por qué no terminó una respuesta, en una frase; `null` si terminó bien. Un motivo nuevo cuenta como error. */
export function endReasonText(line: Pick<ChatLine, 'reason' | 'error'>): string | null {
  const reason = line.reason ?? 'done'
  if (reason === 'done') return null
  const text = ownText(REASON, reason)
  if (text) return text
  return line.error ? `Falló: ${turnErrorText(line.error).toLowerCase()}.` : 'Falló.'
}

/**
 * Qué hacer al cerrarse el socket, según la clase del código (`closeCodes.ts`, nunca el texto):
 * - `stop` (`fix`: el resto de 4000–4899 y 1008): no tiene arreglo reconectando (conversación que ya
 *   no existe, panel desactivado, acceso retirado…). Lo decide la persona, con «Reintentar».
 * - `idle` (`on_input`, 1000): inactividad de 5 min: se reabre al escribir, no antes.
 * - `retry` (`backoff`): caída (1006), fallo del gateway u OpenClaw caído (1011), reinicio (1012),
 *   cliente lento (1013), 49xx (handshake a destiempo…).
 */
export type CloseAction = 'stop' | 'idle' | 'retry'

const ACTION: Record<Reconnect, CloseAction> = {
  fix: 'stop',
  on_input: 'idle',
  backoff: 'retry',
}

export function closeAction(code: number): CloseAction {
  return ACTION[reconnectClass(code)]
}

/** Frases de los cierres que paran el chat (`stop`) y tienen una explicación propia. */
const STOP_TEXT: Partial<Record<number, string>> = {
  4010: 'Tu sesión en este navegador ya no vale (se retiró o te desactivaron). Vuelve a entrar.',
  4011: 'Este navegador no tiene permiso para conversar. Vuelve a entrar.',
  4020: 'El agente de esta conversación no está permitido para este navegador.',
  4021: 'El agente de esta conversación no existe en OpenClaw.',
  4100: 'Esta conversación ya no existe en el gateway.',
  4101: 'El agente no coincide con el de la conversación.',
  4200: 'Se retiró el acceso de este navegador: se cerró su sesión o te desactivaron. Vuelve a entrar.',
}

/** La frase para un cierre que para el chat (`stop`). */
export function stopText(code: number): string {
  return STOP_TEXT[code] ?? 'El gateway no aceptó la conexión del chat.'
}

/** Espera antes de reintentar: 1 s, 2 s, 4 s… hasta 30 s. */
export function backoffMs(failures: number): number {
  return Math.min(30_000, 1000 * 2 ** Math.max(0, failures - 1))
}
