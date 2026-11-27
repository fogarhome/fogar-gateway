// `GET /events` (5.0, docs/client-protocol.md §16): un stream SSE por pestaña con todo lo que pasa
// en las conversaciones que este navegador puede ver. Con `fetch` y no `EventSource`, porque el
// token va como `Authorization: Bearer` y `EventSource` no manda cabeceras. Reconecta solo con
// `Last-Event-ID` (el gateway corta cada stream a los ~25 s): si lo perdido ya no está, `resync`.
// Sin Vue: lo usa el store `events` y se prueba solo.

export interface GatewayEvent {
  type: string
  conversation?: string | null
  [key: string]: unknown
}

export type StreamState =
  | 'connecting'
  | 'ready'
  | 'reconnecting'
  | 'stopped' // 401/403: el token ya no vale o no tiene permiso; no se reintenta

export interface EventStreamOptions {
  token: string
  /** Cada evento, con su id (`null` si no lleva: `ready` al reanudar, `audio`). */
  onEvent: (event: GatewayEvent, id: number | null) => void
  onState?: (state: StreamState) => void
  /** Para las pruebas. */
  fetchImpl?: typeof fetch
  url?: string
}

/** Lee un stream SSE por trozos y devuelve los eventos completos (`id` y `data`). */
export class SseParser {
  private buffer = ''
  private id: number | null = null
  private data: string[] = []

  push(chunk: string): Array<{ id: number | null; data: string }> {
    this.buffer += chunk
    const out: Array<{ id: number | null; data: string }> = []
    let nl: number
    while ((nl = this.buffer.indexOf('\n')) >= 0) {
      const line = this.buffer.slice(0, nl).replace(/\r$/, '')
      this.buffer = this.buffer.slice(nl + 1)
      if (line === '') {
        if (this.data.length) out.push({ id: this.id, data: this.data.join('\n') })
        this.id = null
        this.data = []
      } else if (line.startsWith('data:')) {
        this.data.push(line.slice(5).replace(/^ /, ''))
      } else if (line.startsWith('id:')) {
        const value = Number(line.slice(3).trim())
        this.id = Number.isSafeInteger(value) ? value : null
      } // `retry:`, comentarios (`: ping`) y lo demás: nada
    }
    return out
  }
}

/** Espera antes de reconectar tras un fallo: 1 s, 2 s, 4 s… hasta 30 s. Un corte limpio: ya. */
export function reconnectDelay(failures: number): number {
  return failures === 0 ? 0 : Math.min(30_000, 1000 * 2 ** (failures - 1))
}

export function createEventStream(options: EventStreamOptions) {
  const fetchImpl = options.fetchImpl ?? fetch.bind(globalThis)
  let lastId: number | null = null
  let failures = 0
  let controller: AbortController | null = null
  let timer: ReturnType<typeof setTimeout> | null = null
  let stopped = false
  let state: StreamState = 'connecting'

  function setState(next: StreamState): void {
    if (next === state) return
    state = next
    options.onState?.(next)
  }

  async function run(): Promise<void> {
    controller = new AbortController()
    const headers: Record<string, string> = { Accept: 'text/event-stream', Authorization: `Bearer ${options.token}` }
    if (lastId != null) headers['Last-Event-ID'] = String(lastId)
    let clean = false
    try {
      const response = await fetchImpl(options.url ?? '/events', { headers, signal: controller.signal })
      if (response.status === 401 || response.status === 403) {
        stopped = true
        setState('stopped')
        return
      }
      if (!response.ok || !response.body) throw new Error(`status ${response.status}`)
      const reader = response.body.getReader()
      const decoder = new TextDecoder()
      const parser = new SseParser()
      for (;;) {
        const { value, done } = await reader.read()
        if (done) break
        for (const { id, data } of parser.push(decoder.decode(value, { stream: true }))) {
          let event: GatewayEvent
          try {
            event = JSON.parse(data) as GatewayEvent
          } catch {
            continue
          }
          if (id != null) lastId = id
          if (event.type === 'ready') {
            failures = 0
            setState('ready')
          }
          options.onEvent(event, id)
        }
      }
      clean = true // el gateway cerró el stream a tiempo: se reconecta ya
    } catch {
      if (stopped) return
    }
    if (stopped) return
    // Un corte limpio sigue «listo» mientras reabre (el gateway lo hace cada ~25 s): solo un fallo se nota.
    if (!clean) {
      failures += 1
      setState('reconnecting')
    }
    timer = setTimeout(() => void run(), reconnectDelay(failures))
  }

  return {
    start(): void {
      stopped = false
      setState('connecting')
      void run()
    },
    stop(): void {
      stopped = true
      if (timer) clearTimeout(timer)
      timer = null
      controller?.abort()
    },
    get state(): StreamState {
      return state
    },
  }
}

export type EventStream = ReturnType<typeof createEventStream>
