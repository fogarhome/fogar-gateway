import { reactive } from 'vue'
import { backoffMs, ChatLog, closeAction, stopText, type ServerMessage } from './chat'

// Una conversación en vivo: abre /ws/stream con el token de este navegador (un dispositivo
// personal, 4.0) en esa conversación, manda texto y sigue la respuesta token a token.
// El token va solo en el handshake: nunca se registra ni se enseña.

export type Connection =
  | 'connecting' // abriendo, hasta el `ready`
  | 'ready'
  | 'reconnecting' // se cayó: reintenta solo, con espera creciente
  | 'idle' // cerrada sin error (inactividad): se reabre al escribir
  | 'stopped' // parada: no tiene arreglo reconectando, o se agotaron los reintentos (ver `notice`)

/**
 * Cuánto se reintenta solo (#344): si las caídas seguidas sin llegar a `ready` (1006, 1011, 1012…)
 * duran ya esto, se deja de reintentar y la conversación se para con «Reintentar» a mano. Cubre un
 * reinicio del gateway o de OpenClaw, o un portátil que despierta; un `ready` vuelve a contar.
 */
export const GIVE_UP_AFTER_MS = 120_000
const GIVE_UP_TEXT = 'No llego al gateway tras varios intentos. Comprueba la conexión y reintenta.'

export interface ChatSessionOptions {
  conversationId: string
  token: string
  /** Para las pruebas. */
  WebSocketImpl?: typeof WebSocket
  url?: string
}

export interface ChatSessionState {
  connection: Connection
  /** El agente de la conversación, de `ready`. */
  agent: string | null
  /** OpenClaw se está reconectando (avisos `status` de la sesión). */
  orchestratorDown: boolean
  /** Por qué se paró (`stopped`). */
  notice: string | null
  /** Reintentos seguidos sin llegar a `ready`. */
  failures: number
  log: ChatLog
}

function defaultUrl(): string {
  return `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws/stream`
}

export function createChatSession(options: ChatSessionOptions) {
  const Impl = options.WebSocketImpl ?? WebSocket
  const state = reactive<ChatSessionState>({
    connection: 'connecting',
    agent: null,
    orchestratorDown: false,
    notice: null,
    failures: 0,
    log: new ChatLog(options.conversationId),
  })
  let ws: WebSocket | null = null
  let retry: ReturnType<typeof setTimeout> | null = null
  // Escrito con la conversación dormida: sale al llegar `ready` (su línea, por si no sale).
  let pending: { text: string; key: string } | null = null
  let disposed = false
  // Desde cuándo falla sin llegar a `ready` (null: no está fallando).
  let failingSince: number | null = null

  function connect(): void {
    if (disposed) return
    if (retry) clearTimeout(retry)
    retry = null
    if (state.connection !== 'reconnecting') state.connection = 'connecting'
    state.notice = null
    const socket = new Impl(options.url ?? defaultUrl())
    ws = socket
    socket.onopen = () => {
      socket.send(
        JSON.stringify({
          client_key: options.token,
          protocol: 5,
          input_mode: 'text',
          // `status`: el progreso del turno (`turn_progress`); llegan también los `pipeline_event`, que el chat ignora.
          output_mode: ['text', 'status'],
          conversation: options.conversationId,
        }),
      )
    }
    socket.onmessage = (event) => {
      if (ws !== socket || typeof event.data !== 'string') return
      let msg: ServerMessage
      try {
        msg = JSON.parse(event.data) as ServerMessage
      } catch {
        return
      }
      onMessage(msg)
    }
    socket.onclose = (event) => {
      if (ws !== socket) return
      ws = null
      onClose(event.code)
    }
  }

  function onMessage(msg: ServerMessage): void {
    if (msg.type === 'ready') {
      const m = msg as Extract<ServerMessage, { type: 'ready' }>
      state.connection = 'ready'
      state.failures = 0
      failingSince = null
      // Un `restored` solo llega a quien estaba conectado al volver OpenClaw: al entrar, se parte de cero.
      state.orchestratorDown = false
      state.agent = m.conversation?.agent ?? m.agent
      if (pending != null) {
        ws?.send(JSON.stringify({ type: 'send', text: pending.text }))
        state.log.sent(pending.key)
        pending = null
      }
      return
    }
    if (msg.type === 'status' && msg.service === 'orchestrator') {
      state.orchestratorDown = msg.state !== 'restored'
      return
    }
    state.log.apply(msg)
  }

  function onClose(code: number): void {
    state.log.closeOpenTurns()
    if (disposed) return
    const action = closeAction(code)
    if (action === 'retry') {
      state.failures += 1
      const now = Date.now()
      failingSince ??= now
      if (now - failingSince < GIVE_UP_AFTER_MS) {
        state.connection = 'reconnecting'
        retry = setTimeout(connect, backoffMs(state.failures))
        return
      }
    }
    if (pending) state.log.markUnsent(pending.key) // no se envió: que se vea
    pending = null
    if (action === 'idle') {
      state.connection = 'idle'
      return
    }
    // `stop`, o los reintentos agotados (#344): solo vuelve con «Reintentar».
    state.notice = action === 'stop' ? stopText(code) : GIVE_UP_TEXT
    state.connection = 'stopped'
  }

  /** Manda un mensaje. Con la conversación dormida, la reabre y lo manda al entrar. */
  function send(text: string): boolean {
    // Aproxima el str.strip() del gateway (que también quita \x1c-\x1f y \x85; el \s de JS
    // quita además U+FEFF, que el gateway conserva): lo que allí queda vacío no se manda (no tendría turn_start y descolocaría la cola de mensajes). Antes, los
    // surrogates sueltos, que el gateway también quita (G-r4-1).
    const clean = text.replace(/\p{Cs}/gu, '').replace(/^[\s\x1c-\x1f\x85]+|[\s\x1c-\x1f\x85]+$/g, '')
    if (!clean) return false
    if (state.connection === 'ready' && ws) {
      ws.send(JSON.stringify({ type: 'send', text: clean }))
      state.log.sent(state.log.addUser(clean))
    } else if (state.connection === 'idle') {
      pending = { text: clean, key: state.log.addUser(clean) }
      connect()
    } else return false
    return true
  }

  function cancel(): void {
    if (state.connection === 'ready' && state.log.turnOpen) ws?.send(JSON.stringify({ type: 'cancel' }))
  }

  /** Empieza. La conversación puede estar abierta a la vez en otras pestañas o aparatos (5.0). */
  function start(): void {
    connect()
  }

  /**
   * Lo dicho se ha vuelto a leer de OpenClaw: fuera lo que ya estaba terminado al pedirlo (`keys`, de
   * `state.log.endedKeys()` antes de pedir), que ya viene en ello. Lo que espera a salir se queda.
   */
  function historyReloaded(keys: string[]): void {
    state.log.dropReloaded(keys, pending?.key)
  }

  /** «Reintentar» tras un `stopped`: vuelve a abrir la conversación y cuenta los fallos desde cero. */
  function reconnect(): void {
    if (ws || disposed) return
    state.log = new ChatLog(options.conversationId)
    state.failures = 0
    failingSince = null
    state.connection = 'connecting'
    connect()
  }

  function dispose(): void {
    disposed = true
    if (retry) clearTimeout(retry)
    const socket = ws
    ws = null
    socket?.close(1000)
  }

  return { state, start, send, cancel, reconnect, historyReloaded, dispose }
}

export type ChatSession = ReturnType<typeof createChatSession>
