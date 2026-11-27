import { describe, expect, it, vi } from 'vitest'
import { createEventStream, reconnectDelay, SseParser } from './events'

describe('SseParser', () => {
  it('junta líneas partidas en trozos, con su id, y salta comentarios y retry', () => {
    const p = new SseParser()
    expect(p.push('retry: 1000\n\nid: 7\nda')).toEqual([])
    expect(p.push('ta: {"type":"ready"}\n\n: ping\n\ndata: {"type":"x"}\n\n')).toEqual([
      { id: 7, data: '{"type":"ready"}' },
      { id: null, data: '{"type":"x"}' },
    ])
  })
})

describe('reconnectDelay', () => {
  it('un corte limpio reconecta ya; los fallos esperan cada vez más, hasta 30 s', () => {
    expect([0, 1, 2, 3, 10].map(reconnectDelay)).toEqual([0, 1000, 2000, 4000, 30000])
  })
})

function body(text: string) {
  const bytes = new TextEncoder().encode(text)
  let sent = false
  return {
    getReader: () => ({
      read: async () => (sent ? { done: true, value: undefined } : ((sent = true), { done: false, value: bytes })),
    }),
  }
}

describe('createEventStream', () => {
  it('manda el Bearer y, al reconectar, el Last-Event-ID del último evento', async () => {
    const calls: Array<Record<string, string>> = []
    const events: string[] = []
    let n = 0
    const fetchImpl = vi.fn(async (_url: string, init: { headers: Record<string, string> }) => {
      calls.push(init.headers)
      n += 1
      const text = n === 1 ? 'id: 5\ndata: {"type":"ready"}\n\nid: 6\ndata: {"type":"token"}\n\n' : ''
      if (n > 2) await new Promise(() => undefined) // la tercera se queda abierta
      return { ok: true, status: 200, body: body(text) } as unknown as Response
    })
    const stream = createEventStream({ token: 't0k', fetchImpl: fetchImpl as unknown as typeof fetch, onEvent: (e) => events.push(e.type) })
    stream.start()
    await vi.waitFor(() => expect(calls.length).toBeGreaterThanOrEqual(2))
    stream.stop()
    expect(calls[0]).toMatchObject({ Authorization: 'Bearer t0k' })
    expect(calls[0]!['Last-Event-ID']).toBeUndefined()
    expect(calls[1]!['Last-Event-ID']).toBe('6')
    expect(events).toEqual(['ready', 'token'])
  })

  it('un corte limpio (cada ~25 s) sigue «listo» mientras reabre: solo un fallo dice que reconecta', async () => {
    const states: string[] = []
    let n = 0
    const fetchImpl = vi.fn(async () => {
      n += 1
      if (n === 3) throw new Error('red')
      if (n > 4) await new Promise(() => undefined)
      return { ok: true, status: 200, body: body('data: {"type":"ready"}\n\n') } as unknown as Response
    })
    const stream = createEventStream({ token: 'x', fetchImpl: fetchImpl as unknown as typeof fetch, onEvent: () => undefined, onState: (s) => states.push(s) })
    stream.start()
    await vi.waitFor(() => expect(fetchImpl).toHaveBeenCalledTimes(3))
    stream.stop()
    expect(states).toEqual(['ready', 'reconnecting'])
  })

  it('un 401 lo para sin reintentar', async () => {
    const states: string[] = []
    const fetchImpl = vi.fn(async () => ({ ok: false, status: 401, body: null }) as unknown as Response)
    const stream = createEventStream({ token: 'x', fetchImpl, onEvent: () => undefined, onState: (s) => states.push(s) })
    stream.start()
    await vi.waitFor(() => expect(states).toContain('stopped'))
    expect(fetchImpl).toHaveBeenCalledTimes(1)
  })
})
