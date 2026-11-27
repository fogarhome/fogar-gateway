import { computed, reactive } from 'vue'
import { ApiError } from '@/api/http'
import type { ConversationLive } from '@/api/types'
import { ChatLog, type ServerMessage } from './chat'
import type { GatewayEvent, StreamState } from './events'
import { turnErrorText } from './labels'

// Una conversación en vivo (5.0, docs/client-protocol.md §16): su estado al entrar
// (`GET /conversations/{id}`, con `as_of`) y, desde ahí, lo que llega por el `/events` de la
// pestaña. Escribir es un POST; parar y quitar, también. Sin Vue Router ni store: se prueba solo.

export type Connection =
  | 'connecting' // abriendo /events o leyendo el estado al entrar
  | 'ready'
  | 'reconnecting' // /events se cortó: reconecta solo
  | 'stopped' // el token ya no vale o no tiene permiso (ver `notice`)

export interface ChatDeps {
  /** El `/events` de la pestaña: escuchar y su estado. */
  subscribe: (listener: (event: GatewayEvent, id: number | null) => void) => () => void
  streamState: () => StreamState
  getLive: (id: string) => Promise<ConversationLive>
  postMessage: (id: string, text: string, interrupt: boolean) => Promise<{ message_id: string }>
  cancelTurn: (id: string) => Promise<{ cancelled: boolean }>
  withdrawMessage: (id: string, messageId: string) => Promise<void>
}

export interface ChatSessionState {
  readonly connection: Connection
  /** El estado al entrar ya está leído: lo siguiente llega por /events. */
  loaded: boolean
  /** Algo que no salió (un envío, parar, quitar), en una frase. */
  notice: string | null
  /** Un envío en marcha (esperando el `202`). */
  sending: boolean
  /** Sube con cada `resync`: hay que volver a leer lo dicho. */
  epoch: number
  log: ChatLog
}

/** Lo que el gateway quita a los extremos (str.strip(), más los surrogates sueltos, G-r4-1). */
export function cleanText(text: string): string {
  return text.replace(/\p{Cs}/gu, '').replace(/^[\s\x1c-\x1f\x85]+|[\s\x1c-\x1f\x85]+$/g, '')
}

function errorText(e: unknown, what: string): string {
  if (e instanceof ApiError) {
    if (e.status === 403) return `No tienes permiso para ${what} aquí.`
    if (e.status === 404) return 'Esta conversación ya no existe.'
    if (e.status === 502) return `${turnErrorText(e.message)}.`
    if (e.status === 503) return 'La conversación está ocupada o OpenClaw no responde: prueba otra vez.'
    if (e.kind === 'network') return 'No llego al gateway.'
  }
  return `No se pudo ${what}.`
}

export function createChatSession(conversationId: string, deps: ChatDeps) {
  let asOf = Number.POSITIVE_INFINITY // hasta leer el estado: nada se aplica
  let held: Array<[GatewayEvent, number | null]> = []
  let unsubscribe: (() => void) | null = null
  let disposed = false
  let loads = 0

  const state = reactive({
    loaded: false,
    notice: null as string | null,
    sending: false,
    epoch: 0,
    log: new ChatLog(conversationId),
    connection: computed<Connection>(() => {
      const stream = deps.streamState()
      if (stream === 'stopped') return 'stopped'
      if (stream === 'reconnecting') return 'reconnecting'
      return stream === 'ready' && state.loaded ? 'ready' : 'connecting'
    }),
  }) as ChatSessionState

  function mine(event: GatewayEvent): boolean {
    return event.conversation === conversationId
  }

  function onEvent(event: GatewayEvent, id: number | null): void {
    if (event.type === 'resync') {
      // Lo perdido ya no está en el gateway: se vuelve a leer todo.
      state.log.closeOpenTurns()
      state.epoch += 1
      void load()
      return
    }
    if (!mine(event)) return
    if (!state.loaded) {
      held.push([event, id])
      return
    }
    if (id != null && id <= asOf) return // ya venía en el estado al entrar
    state.log.apply(event as ServerMessage)
  }

  /** Lee el estado al entrar y aplica lo que llegó mientras tanto con id > `as_of`. */
  async function load(): Promise<void> {
    const mineLoad = ++loads
    state.loaded = false
    try {
      const live = await deps.getLive(conversationId)
      if (disposed || mineLoad !== loads) return
      asOf = live.as_of
      state.log.clear()
      for (const message of live.pending) state.log.apply(message as ServerMessage)
      if (live.turn) {
        state.log.apply({ ...live.turn, type: 'turn_start' } as ServerMessage)
        if (live.turn.text) state.log.apply({ type: 'token', turn_id: live.turn.turn_id, text: live.turn.text })
      }
      state.loaded = true
      const pending = held
      held = []
      for (const [event, id] of pending) onEvent(event, id)
    } catch (e) {
      if (disposed || mineLoad !== loads) return
      state.notice = errorText(e, 'abrir la conversación')
    }
  }

  async function cancel(): Promise<void> {
    try {
      await deps.cancelTurn(conversationId)
    } catch (e) {
      state.notice = errorText(e, 'parar')
    }
  }

  return {
    state,
    start(): void {
      unsubscribe = deps.subscribe(onEvent)
      void load()
    },
    /** Escribe. `interrupt`: enviar ahora (para el turno en curso y responde a este). */
    async send(text: string, interrupt = false): Promise<boolean> {
      const clean = cleanText(text)
      if (!clean || state.sending || state.connection !== 'ready') return false
      state.sending = true
      state.notice = null
      try {
        await deps.postMessage(conversationId, clean, interrupt)
        return true // su línea llega por /events (`message`)
      } catch (e) {
        state.notice = errorText(e, 'enviar el mensaje')
        return false
      } finally {
        state.sending = false
      }
    },
    cancel,
    /**
     * «Enviar ahora» en el primero de la cola: para el turno que se está respondiendo, y el mensaje
     * sale detrás. Sin turno abierto no hace nada: el mensaje ya va a salir, y `/cancel` sin turno
     * pararía justo ese (el siguiente pendiente, docs/client-protocol.md §4). `interrupt` no sirve:
     * va en un POST nuevo, así que reenviaría el texto (duplicado) y en ese hueco también para el siguiente.
     */
    async sendNow(): Promise<void> {
      if (state.log.turnOpen) await cancel()
    },
    async withdraw(messageId: string): Promise<void> {
      try {
        await deps.withdrawMessage(conversationId, messageId)
      } catch (e) {
        if (e instanceof ApiError && e.status === 404) return // ya no estaba pendiente
        state.notice = errorText(e, 'quitar el mensaje')
      }
    },
    /** Lo dicho se ha vuelto a leer: fuera lo terminado antes de pedirlo (`keys`). */
    historyReloaded(keys: string[]): void {
      state.log.dropReloaded(keys)
    },
    /** «Reintentar»: vuelve a leer el estado. */
    reconnect(): void {
      state.notice = null
      void load()
    },
    dispose(): void {
      disposed = true
      unsubscribe?.()
      unsubscribe = null
    },
  }
}

export type ChatSession = ReturnType<typeof createChatSession>
