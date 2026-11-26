import { defineStore } from 'pinia'
import { reactive, ref } from 'vue'
import { createEventStream, type EventStream, type GatewayEvent, type StreamState } from '@/lib/events'
import { useSessionStore } from '@/stores/session'

export type EventListener = (event: GatewayEvent, id: number | null) => void

/**
 * El `/events` de esta pestaña (5.0): uno solo para todas las conversaciones. Lo abre quien lo
 * necesite (`ensure`) y lo reparte a quien escuche; además sabe qué conversaciones tienen un turno
 * en curso («activa ahora» en la lista).
 */
export const useEventsStore = defineStore('events', () => {
  const connection = ref<StreamState>('stopped')
  /** Conversaciones con un turno abierto ahora (por los `turn_start`/`turn_end` que llegan). */
  const active = reactive(new Set<string>())
  const listeners = new Set<EventListener>()
  const openTurns = new Map<string, string>() // turn_id → conversación
  let stream: EventStream | null = null
  let token: string | null = null

  function dispatch(event: GatewayEvent, id: number | null): void {
    if (event.type === 'turn_start' && typeof event.turn_id === 'string' && event.conversation) {
      openTurns.set(event.turn_id, event.conversation)
      active.add(event.conversation)
    } else if (event.type === 'turn_end' && typeof event.turn_id === 'string') {
      const conversation = openTurns.get(event.turn_id)
      openTurns.delete(event.turn_id)
      if (conversation && ![...openTurns.values()].includes(conversation)) active.delete(conversation)
    } else if (event.type === 'resync') {
      openTurns.clear()
      active.clear()
    }
    for (const listener of [...listeners]) listener(event, id)
  }

  /** Abre el stream con el token de la sesión (o lo reabre si cambió). */
  function ensure(): void {
    const current = useSessionStore().token
    if (!current) return stop()
    if (stream && token === current) return
    stop()
    token = current
    stream = createEventStream({
      token: current,
      onEvent: dispatch,
      onState: (state) => (connection.value = state),
    })
    stream.start()
  }

  function stop(): void {
    stream?.stop()
    stream = null
    token = null
    connection.value = 'stopped'
    openTurns.clear()
    active.clear()
  }

  function subscribe(listener: EventListener): () => void {
    listeners.add(listener)
    return () => listeners.delete(listener)
  }

  return { connection, active, ensure, stop, subscribe }
})
