import { describe, expect, it, vi } from 'vitest'
import { ref } from 'vue'
import { ApiError } from '@/api/http'
import type { ConversationLive } from '@/api/types'
import { createChatSession, type ChatDeps } from './chatSession'
import type { GatewayEvent, StreamState } from './events'

// El chat de una conversación por HTTP (5.0): el estado al entrar, `as_of` y lo que llega por /events.

const LIVE = (extra: Partial<ConversationLive> = {}): ConversationLive =>
  ({ id: 'c1', as_of: 10, turn: null, pending: [], ...extra }) as ConversationLive
const message = (id: string, state = 'pending') => ({
  type: 'message', id, conversation: 'c1', text: id, state, from: { source: 'device', device_id: 'yo', name: 'Mac', self: true },
})
const flush = () => new Promise((r) => setTimeout(r, 0))

function harness(live: ConversationLive = LIVE()) {
  let listener: ((e: GatewayEvent, id: number | null) => void) | null = null
  const stream = ref<StreamState>('ready')
  const deps: ChatDeps = {
    subscribe: (fn) => {
      listener = fn
      return () => (listener = null)
    },
    streamState: () => stream.value,
    getLive: vi.fn(async () => live),
    postMessage: vi.fn(async () => ({ message_id: 'run-1' })),
    cancelTurn: vi.fn(async () => ({ cancelled: true })),
    withdrawMessage: vi.fn(async () => undefined),
  }
  const chat = createChatSession('c1', deps)
  const emit = (event: object, id: number | null = null) => listener?.(event as GatewayEvent, id)
  return { chat, deps, stream, emit }
}

describe('createChatSession', () => {
  it('entra con el estado leído (turno en curso y pendientes) y descarta lo que ya traía (id ≤ as_of)', async () => {
    const h = harness(LIVE({ turn: { turn_id: 'T', origin: 'user', text: 'lleva' }, pending: [message('m2')] }))
    h.chat.start()
    h.emit({ type: 'token', turn_id: 'T', conversation: 'c1', text: 'antes' }, 9) // mientras carga
    await flush()
    expect(h.chat.state.connection).toBe('ready')
    h.emit({ type: 'token', turn_id: 'T', conversation: 'c1', text: ' más' }, 11)
    h.emit({ type: 'token', turn_id: 'T', conversation: 'otra', text: 'ajeno' }, 12)
    const lines = h.chat.state.log.lines
    expect(lines.map((l) => l.text)).toEqual(['m2', 'lleva más'])
  })

  it('escribe con un POST; la línea llega por su `message`', async () => {
    const h = harness()
    h.chat.start()
    await flush()
    expect(await h.chat.send('  hola  ')).toBe(true)
    expect(h.deps.postMessage).toHaveBeenCalledWith('c1', 'hola', false)
    expect(h.chat.state.log.lines).toHaveLength(0)
    h.emit(message('run-1'), 11)
    expect(h.chat.state.log.pending.map((l) => l.messageId)).toEqual(['run-1'])
    expect(await h.chat.send('   ')).toBe(false)
  })

  it('no escribe hasta que /events esté listo, y un fallo del POST se dice', async () => {
    const h = harness()
    h.stream.value = 'reconnecting'
    h.chat.start()
    await flush()
    expect(h.chat.state.connection).toBe('reconnecting')
    expect(await h.chat.send('hola')).toBe(false)
    h.stream.value = 'ready'
    h.deps.postMessage = vi.fn(async () => {
      throw new ApiError('server', 502, 'orchestrator_error')
    })
    const chat = createChatSession('c1', h.deps)
    chat.start()
    await flush()
    expect(await chat.send('hola')).toBe(false)
    expect(chat.state.notice).toBe('OpenClaw rechazó la pregunta.')
  })

  it('parar y quitar van por la API; quitar uno que ya no espera no es un error', async () => {
    const h = harness()
    h.chat.start()
    await flush()
    await h.chat.cancel()
    expect(h.deps.cancelTurn).toHaveBeenCalledWith('c1', undefined) // «Parar»: sin turn_id, lo que haya
    h.deps.withdrawMessage = vi.fn(async () => {
      throw new ApiError('not-found', 404, 'not_pending')
    })
    const chat = createChatSession('c1', h.deps)
    chat.start()
    await flush()
    await chat.withdraw('m1')
    expect(chat.state.notice).toBeNull()
  })

  it('los códigos de error salen en una frase, también los desconocidos', async () => {
    const h = harness()
    h.deps.postMessage = vi.fn(async () => {
      throw new ApiError('server', 502, 'orchestrator_rate_limited')
    })
    h.chat.start()
    await flush()
    await h.chat.send('hola')
    expect(h.chat.state.notice).toBe('El modelo está saturado o sin cuota: prueba más tarde.')
    h.deps.postMessage = vi.fn(async () => {
      throw new ApiError('server', 502, 'codigo_nuevo')
    })
    const chat = createChatSession('c1', h.deps)
    chat.start()
    await flush()
    await chat.send('hola')
    expect(chat.state.notice).toBe('OpenClaw rechazó la pregunta.')
  })

  it('«Enviar ahora» solo para si hay un turno abierto: sin él pararía el propio mensaje', async () => {
    const h = harness()
    h.chat.start()
    await flush()
    h.emit(message('m2'), 11)
    await h.chat.sendNow()
    expect(h.deps.cancelTurn).not.toHaveBeenCalled()
    h.emit({ type: 'turn_start', turn_id: 'T', conversation: 'c1', origin: 'user' }, 12)
    await h.chat.sendNow()
    // con el turno que ve: si acaba antes de que llegue el POST, el gateway no para el mensaje
    expect(h.deps.cancelTurn).toHaveBeenCalledWith('c1', 'T')
  })

  it('«Enviar ahora» con solo un aviso de otra conversación abierto no para nada', async () => {
    const h = harness()
    h.chat.start()
    await flush()
    h.emit(message('m2'), 11)
    h.emit({ type: 'turn_start', turn_id: 'P', conversation: 'otra', origin: 'push' }, 12)
    await h.chat.sendNow()
    expect(h.deps.cancelTurn).not.toHaveBeenCalled()
  })

  it('un resync cierra lo abierto, vuelve a leer el estado y pide releer lo dicho', async () => {
    const h = harness()
    h.chat.start()
    await flush()
    h.emit({ type: 'turn_start', turn_id: 'T', conversation: 'c1', origin: 'user' }, 11)
    h.emit({ type: 'resync' }, 50)
    expect(h.chat.state.epoch).toBe(1)
    await flush()
    expect(h.deps.getLive).toHaveBeenCalledTimes(2)
    expect(h.chat.state.log.lines).toHaveLength(0)
  })

  it('con el token retirado, se para', async () => {
    const h = harness()
    h.stream.value = 'stopped'
    h.chat.start()
    await flush()
    expect(h.chat.state.connection).toBe('stopped')
  })
})
