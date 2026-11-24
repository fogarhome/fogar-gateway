import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createChatSession, MAX_CONSECUTIVE_FAILURES } from './chatSession'

// Un WebSocket de mentira: guarda lo enviado y deja simular lo que manda el gateway.
class FakeSocket {
  static all: FakeSocket[] = []
  sent: unknown[] = []
  closedWith: number | null = null
  onopen: (() => void) | null = null
  onmessage: ((e: { data: unknown }) => void) | null = null
  onclose: ((e: { code: number; reason: string }) => void) | null = null
  constructor(readonly url: string) {
    FakeSocket.all.push(this)
  }
  send(data: string) {
    this.sent.push(JSON.parse(data))
  }
  close(code: number) {
    this.closedWith = code
  }
  // Lado del gateway
  open() {
    this.onopen?.()
  }
  receive(msg: object) {
    this.onmessage?.({ data: JSON.stringify(msg) })
  }
  serverClose(code: number, reason = '') {
    this.onclose?.({ code, reason })
  }
}

const last = () => FakeSocket.all.at(-1)!
const READY = { type: 'ready', session_id: 'panel-web:1', agent: 'main', conversation: { id: 'c1', agent: 'main' } }

function session() {
  const s = createChatSession({
    conversationId: 'c1',
    token: 'secreto',
    url: 'ws://gw/ws/stream',
    WebSocketImpl: FakeSocket as unknown as typeof WebSocket,
  })
  s.start()
  last().open()
  last().receive(READY)
  return s
}

describe('createChatSession', () => {
  beforeEach(() => {
    FakeSocket.all = []
    vi.useFakeTimers()
  })
  afterEach(() => vi.useRealTimers())

  it('entra con el token de este navegador (su client_key) en su conversación, sin pedir agente', () => {
    const s = session()
    expect(last().sent[0]).toEqual({ client_key: 'secreto', protocol: 5, input_mode: 'text', output_mode: ['text', 'status'], conversation: 'c1' })
    expect(s.state.connection).toBe('ready')
    expect(s.state.agent).toBe('main')
  })

  it('manda lo escrito y sigue la respuesta token a token', () => {
    const s = session()
    expect(s.send('  Hola  ')).toBe(true)
    expect(last().sent.at(-1)).toEqual({ type: 'send', text: 'Hola' })
    last().receive({ type: 'turn_start', turn_id: 't-1', turn_seq: 1, conversation: 'c1', origin: 'user' })
    last().receive({ type: 'token', turn_id: 't-1', text: '¡Hola!' })
    expect(s.state.log.turnOpen).toBe(true)
    s.cancel()
    expect(last().sent.at(-1)).toEqual({ type: 'cancel' })
    last().receive({ type: 'turn_end', turn_id: 't-1', reason: 'cancelled' })
    expect(s.state.log.lines.map((l) => [l.role, l.text, l.reason])).toEqual([
      ['user', 'Hola', 'done'],
      ['assistant', '¡Hola!', 'cancelled'],
    ])
  })

  it('limpia el texto como el gateway: lo que allí queda vacío no se manda', () => {
    const s = session()
    const before = last().sent.length
    expect(s.send('\x85\x1c \x1f')).toBe(false)
    expect(last().sent.length).toBe(before)
    expect(s.send('\x1dHola\x85')).toBe(true)
    expect(last().sent.at(-1)).toEqual({ type: 'send', text: 'Hola' })
  })

  it('quita los surrogates sueltos como el gateway, antes de los espacios (G-r4-1)', () => {
    const s = session()
    const before = last().sent.length
    expect(s.send('\uD800 \uDC00')).toBe(false)
    expect(last().sent.length).toBe(before)
    expect(s.send(' \uD800Hola \uDFFF')).toBe(true)
    expect(last().sent.at(-1)).toEqual({ type: 'send', text: 'Hola' })
    expect(s.send('\u{1F600}')).toBe(true) // un par válido no es un surrogate suelto
    last().serverClose(1000) // camino pending: la conversación dormida tampoco lo guarda
    expect(s.send('\uDC00')).toBe(false)
    expect(FakeSocket.all).toHaveLength(1)
  })

  it('enseña en qué está la respuesta hasta el primer token; los pipeline_event no cambian nada', () => {
    const s = session()
    s.send('enciende la luz')
    last().receive({ type: 'turn_start', turn_id: 't-1', turn_seq: 1, conversation: 'c1', origin: 'user' })
    last().receive({ type: 'pipeline_event', stage: 'llm_start', elapsed_ms: 2, turn: 1 })
    last().receive({ type: 'turn_progress', turn_id: 't-1', phase: 'tool', tool: 'ha_call_service' })
    expect(s.state.log.lines[1]!.progress).toEqual({ phase: 'tool', tool: 'ha_call_service' })
    last().receive({ type: 'token', turn_id: 't-1', text: 'Hecho.' })
    expect(s.state.log.lines[1]).toMatchObject({ text: 'Hecho.', progress: null })
  })

  it('un push llega como Fogar hablando primero', () => {
    const s = session()
    last().receive({ type: 'turn_start', turn_id: 't-1', turn_seq: 1, conversation: 'c1', origin: 'push' })
    expect(s.state.log.lines[0]).toMatchObject({ role: 'assistant', origin: 'push', state: 'streaming' })
  })

  it('parada, «Reintentar» vuelve a abrir la conversación desde cero', () => {
    const s = session()
    last().receive({ type: 'turn_start', turn_id: 't-1', turn_seq: 1, conversation: 'c1', origin: 'user' })
    last().receive({ type: 'turn_end', turn_id: 't-1', reason: 'closed' })
    last().serverClose(4100, 'conversation not found')
    expect(s.state.connection).toBe('stopped')
    vi.advanceTimersByTime(120_000)
    expect(FakeSocket.all).toHaveLength(1)
    expect(s.send('hola')).toBe(false)

    s.reconnect()
    expect(FakeSocket.all).toHaveLength(2)
    expect(s.state.log.lines).toEqual([]) // se vuelve a leer lo dicho
    last().open()
    last().receive(READY)
    expect(s.state.connection).toBe('ready')
  })

  it(`tras ${MAX_CONSECUTIVE_FAILURES} caídas seguidas deja de reintentar solo y espera a «Reintentar» (#344)`, () => {
    const s = session()
    s.send('¿Llegas?')
    for (let i = 1; i < MAX_CONSECUTIVE_FAILURES; i++) {
      last().serverClose([1006, 1011, 1012][i % 3]!)
      expect(s.state.connection).toBe('reconnecting')
      vi.advanceTimersByTime(30_000)
      expect(FakeSocket.all).toHaveLength(i + 1)
      last().open() // abre, pero no llega a `ready`
    }
    last().serverClose(1006)
    expect(s.state.connection).toBe('stopped')
    expect(s.state.notice).toMatch(/tras varios intentos/)
    expect(s.state.failures).toBe(MAX_CONSECUTIVE_FAILURES)
    vi.advanceTimersByTime(600_000)
    expect(FakeSocket.all).toHaveLength(MAX_CONSECUTIVE_FAILURES) // nada más, solo
    expect(s.send('hola')).toBe(false)

    s.reconnect()
    expect(s.state.failures).toBe(0)
    expect(FakeSocket.all).toHaveLength(MAX_CONSECUTIVE_FAILURES + 1)
    last().open()
    last().receive(READY)
    expect(s.state.connection).toBe('ready')
  })

  it('un `ready` vuelve a contar las caídas desde cero', () => {
    const s = session()
    for (let round = 0; round < 3; round++) {
      for (let i = 1; i < MAX_CONSECUTIVE_FAILURES; i++) {
        last().serverClose(1006)
        expect(s.state.connection).toBe('reconnecting')
        vi.advanceTimersByTime(30_000)
      }
      last().open()
      last().receive(READY)
      expect(s.state.connection).toBe('ready')
      expect(s.state.failures).toBe(0)
    }
  })

  it('tras una caída reintenta solo, con espera, y cierra el turno que quedó abierto', () => {
    const s = session()
    last().receive({ type: 'turn_start', turn_id: 't-1', turn_seq: 1, conversation: 'c1', origin: 'user' })
    last().serverClose(1012)
    expect(s.state.connection).toBe('reconnecting')
    expect(s.state.log.lines[0]).toMatchObject({ state: 'ended', reason: 'closed' })
    vi.advanceTimersByTime(999)
    expect(FakeSocket.all).toHaveLength(1)
    vi.advanceTimersByTime(1)
    expect(FakeSocket.all).toHaveLength(2)
    last().open()
    expect(last().sent[0]).toMatchObject({ conversation: 'c1' }) // la misma conversación
    last().receive(READY)
    expect(s.state.connection).toBe('ready')
    expect(s.state.failures).toBe(0)
  })

  it('cerrada por inactividad, se reabre al escribir y manda el mensaje al entrar', () => {
    const s = session()
    last().serverClose(1000)
    expect(s.state.connection).toBe('idle')
    vi.advanceTimersByTime(60_000)
    expect(FakeSocket.all).toHaveLength(1)
    expect(s.send('¿Sigues ahí?')).toBe(true)
    expect(FakeSocket.all).toHaveLength(2)
    last().open()
    expect(last().sent).toHaveLength(1) // solo el handshake hasta el ready
    last().receive(READY)
    expect(last().sent.at(-1)).toEqual({ type: 'send', text: '¿Sigues ahí?' })
  })

  it('lo escrito con la conversación dormida se marca «no enviado» si no llega a entrar', () => {
    const s = session()
    last().serverClose(1000)
    s.send('¿Hola?')
    last().open()
    last().serverClose(4200)
    expect(s.state.connection).toBe('stopped')
    expect(s.state.log.lines.at(-1)).toMatchObject({ text: '¿Hola?', unsent: true })
  })

  it('releer lo dicho con un mensaje esperando a la conversación dormida no lo quita', () => {
    const s = session()
    last().serverClose(1000)
    s.send('¿Hola?')
    s.historyReloaded(s.state.log.endedKeys())
    expect(s.state.log.lines.map((l) => l.text)).toEqual(['¿Hola?'])
  })

  it('lo pendiente que sale en el ready durante una recarga no se quita', () => {
    const s = session()
    last().serverClose(1000)
    s.send('¿Hola?')
    const keys = s.state.log.endedKeys() // se pide la historia ahora…
    last().open()
    last().receive(READY) // …y sale mientras llega
    s.historyReloaded(keys)
    expect(s.state.log.lines.map((l) => l.text)).toEqual(['¿Hola?'])
  })

  it('una caída con un mensaje sin turn_start lo deja «no enviado»', () => {
    const s = session()
    s.send('¿Llegas?')
    last().serverClose(1006)
    expect(s.state.connection).toBe('reconnecting')
    expect(s.state.log.lines.at(-1)).toMatchObject({ text: '¿Llegas?', unsent: true })
  })

  it('al volver a entrar, el aviso de OpenClaw caído empieza de cero', () => {
    const s = session()
    last().receive({ type: 'status', service: 'orchestrator', state: 'reconnecting' })
    last().serverClose(1006)
    vi.advanceTimersByTime(1000)
    last().open()
    last().receive(READY)
    expect(s.state.orchestratorDown).toBe(false)
  })

  it('con el acceso retirado no reintenta solo', () => {
    const s = session()
    last().serverClose(4200, 'credential revoked')
    expect(s.state.connection).toBe('stopped')
    vi.advanceTimersByTime(60_000)
    expect(FakeSocket.all).toHaveLength(1)
  })

  it('para con un 4xxx que no se arregla reconectando y dice por qué, por el código', () => {
    const s = session()
    last().serverClose(4100, 'texto que puede cambiar')
    expect(s.state.connection).toBe('stopped')
    expect(s.state.notice).toMatch(/ya no existe/)
    vi.advanceTimersByTime(60_000)
    expect(FakeSocket.all).toHaveLength(1)
  })

  it('sigue los avisos de OpenClaw y al salir cierra sin reintentar', () => {
    const s = session()
    last().receive({ type: 'status', service: 'orchestrator', state: 'reconnecting' })
    expect(s.state.orchestratorDown).toBe(true)
    last().receive({ type: 'status', service: 'orchestrator', state: 'restored' })
    expect(s.state.orchestratorDown).toBe(false)
    const socket = last()
    s.dispose()
    expect(socket.closedWith).toBe(1000)
    socket.serverClose(1000)
    vi.advanceTimersByTime(60_000)
    expect(FakeSocket.all).toHaveLength(1)
  })
})
