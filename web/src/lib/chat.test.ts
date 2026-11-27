import { describe, expect, it } from 'vitest'
import { backoffMs, ChatLog, closeAction, endReasonText, progressText, stopText } from './chat'

const start = (turn_id: string, extra: object = {}) => ({ type: 'turn_start', turn_id, turn_seq: 1, conversation: 'c1', origin: 'user', ...extra })

describe('ChatLog', () => {
  const msg = (id: string, state = 'pending', self = true, extra: object = {}) => ({
    type: 'message', id, conversation: 'c1', text: `texto ${id}`, state,
    from: { source: 'device', device_id: self ? 'yo' : 'tele', name: self ? 'Mac' : 'Tele', self }, ...extra,
  })

  it('el mensaje llega por su `message` y la respuesta junta sus tokens hasta su turn_end', () => {
    const log = new ChatLog('c1')
    log.apply(msg('m1'))
    log.apply(msg('m1', 'delivered'))
    log.apply(start('t-1'))
    log.apply({ type: 'token', turn_id: 't-1', text: 'Sol' })
    expect(log.turnOpen).toBe(true)
    log.apply({ type: 'token', turn_id: 't-1', text: ' y 20 grados.' })
    log.apply({ type: 'turn_end', turn_id: 't-1', reason: 'done' })
    expect(log.turnOpen).toBe(false)
    expect(log.lines.map((l) => [l.role, l.text, l.messageState ?? l.state])).toEqual([
      ['user', 'texto m1', 'delivered'],
      ['assistant', 'Sol y 20 grados.', 'ended'],
    ])
  })

  it('los mensajes de otro aparato llevan su nombre; los pendientes se pueden listar y se quitan', () => {
    const log = new ChatLog('c1')
    log.apply(msg('m1', 'pending', false))
    log.apply(msg('m2', 'pending', true))
    expect(log.lines.map((l) => l.from)).toEqual([{ name: 'Tele', self: false }, { name: 'Mac', self: true }])
    expect(log.pending.map((l) => l.messageId)).toEqual(['m1', 'm2'])
    log.apply(msg('m1', 'withdrawn', false))
    expect(log.pending.map((l) => l.messageId)).toEqual(['m2'])
  })

  it('guarda por qué terminó un turno: cancelado, timeout tras su error', () => {
    const log = new ChatLog('c1')
    log.apply(start('t-1'))
    log.apply({ type: 'turn_end', turn_id: 't-1', reason: 'cancelled' })
    log.apply(start('t-2'))
    log.apply({ type: 'error', code: 'TURN_ERROR', message: 'turn_timeout', fatal: false, turn_id: 't-2' })
    log.apply({ type: 'turn_end', turn_id: 't-2', reason: 'timeout' })
    expect(log.lines.map((l) => [l.reason, l.error])).toEqual([
      ['cancelled', undefined],
      ['timeout', 'turn_timeout'],
    ])
  })

  it('no acepta tokens de un turno ya cerrado ni de uno desconocido, ni repite un turn_start', () => {
    const log = new ChatLog('c1')
    log.apply(start('t-1'))
    expect(log.apply(start('t-1'))).toBe(false)
    log.apply({ type: 'turn_end', turn_id: 't-1', reason: 'interrupted' })
    expect(log.apply({ type: 'token', turn_id: 't-1', text: 'tarde' })).toBe(false)
    expect(log.apply({ type: 'token', turn_id: 't-9', text: 'nadie' })).toBe(false)
    expect(log.lines).toHaveLength(1)
  })

  it('marca el push como Fogar hablando primero y avisa si es de otra conversación', () => {
    const log = new ChatLog('c1')
    log.apply(start('t-1', { origin: 'push' }))
    log.apply(start('t-2', { origin: 'push', conversation: 'ayer' }))
    expect(log.lines.map((l) => [l.origin, l.otherConversation])).toEqual([
      ['push', undefined],
      ['push', 'ayer'],
    ])
  })
})

describe('ChatLog al volver a leer lo dicho', () => {
  const texts = (log: ChatLog) => log.lines.map((l) => l.text)

  it('quita lo terminado y lo entregado antes de pedir; deja lo pendiente y lo que sigue llegando', () => {
    const log = new ChatLog('c1')
    const message = (id: string, state: string) =>
      ({ type: 'message', id, conversation: 'c1', text: id, state, from: { source: 'device', device_id: 'yo', name: 'Mac', self: true } })
    log.apply(message('entregado', 'delivered'))
    log.apply(start('t-1'))
    log.apply({ type: 'token', turn_id: 't-1', text: 'respuesta' })
    log.apply({ type: 'turn_end', turn_id: 't-1', reason: 'done' })
    log.apply(message('pendiente', 'pending'))
    log.apply(start('t-2'))
    const keys = log.endedKeys()
    log.dropReloaded(keys)
    expect(texts(log)).toEqual(['pendiente', ''])
  })

  it('se queda con el turno que no llegó a OpenClaw', () => {
    const log = new ChatLog('c1')
    log.apply(start('t-1'))
    log.apply({ type: 'error', code: 'TURN_ERROR', message: 'orchestrator_unavailable', fatal: false, turn_id: 't-1' })
    log.apply({ type: 'turn_end', turn_id: 't-1', reason: 'error' })
    log.dropReloaded(log.endedKeys())
    expect(log.lines).toHaveLength(1)
  })
})

describe('ChatLog con turn_progress', () => {
  const progress = (turn_id: string, phase: string, tool: string | null = null) => ({ type: 'turn_progress', turn_id, phase, tool })

  it('guarda en qué está la respuesta mientras no llega texto; cada uno sustituye al anterior', () => {
    const log = new ChatLog('c1')
    log.apply(start('t-1'))
    expect(log.apply(progress('t-1', 'preparing'))).toBe(true)
    expect(log.lines[0]!.progress).toEqual({ phase: 'preparing', tool: null })
    log.apply(progress('t-1', 'tool', 'ha_call_service'))
    expect(log.lines[0]!.progress).toEqual({ phase: 'tool', tool: 'ha_call_service' })
  })

  it('el primer token lo quita, y vuelve si usa otra herramienta a media respuesta', () => {
    const log = new ChatLog('c1')
    log.apply(start('t-1'))
    log.apply(progress('t-1', 'thinking'))
    log.apply({ type: 'token', turn_id: 't-1', text: 'Voy' })
    expect(log.lines[0]!.progress).toBeNull()
    log.apply(progress('t-1', 'tool', 'read'))
    expect(log.lines[0]!.progress).toEqual({ phase: 'tool', tool: 'read' })
  })

  it('el turn_end lo quita, y el cierre del socket también', () => {
    const log = new ChatLog('c1')
    log.apply(start('t-1'))
    log.apply(progress('t-1', 'thinking'))
    log.apply({ type: 'turn_end', turn_id: 't-1', reason: 'done' })
    expect(log.lines[0]!.progress).toBeNull()
    log.apply(start('t-2'))
    log.apply(progress('t-2', 'thinking'))
    log.closeOpenTurns()
    expect(log.lines[1]!.progress).toBeNull()
  })

  it('no acepta progreso de un turno cerrado ni de uno desconocido', () => {
    const log = new ChatLog('c1')
    log.apply(start('t-1'))
    log.apply({ type: 'turn_end', turn_id: 't-1', reason: 'cancelled' })
    expect(log.apply(progress('t-1', 'thinking'))).toBe(false)
    expect(log.apply(progress('t-9', 'thinking'))).toBe(false)
    expect(log.lines[0]!.progress).toBeNull()
  })

  it('pipeline_event no cambia nada de la conversación', () => {
    const log = new ChatLog('c1')
    log.apply(start('t-1'))
    expect(log.apply({ type: 'pipeline_event', stage: 'llm_start', elapsed_ms: 3, turn: 1 })).toBe(false)
  })
})

describe('progressText', () => {
  it('dice cada fase en una frase corta', () => {
    expect(progressText({ phase: 'preparing', tool: null })).toBe('Preparando…')
    expect(progressText({ phase: 'thinking', tool: null })).toBe('Pensando…')
    expect(progressText({ phase: 'tool', tool: 'ha_call_service' })).toBe('Usando ha_call_service…')
  })
  it('una herramienta sin nombre es «una herramienta»', () => {
    expect(progressText({ phase: 'tool', tool: null })).toBe('Usando una herramienta…')
  })
  it('una fase que la web no conoce es un «trabajando» genérico', () => {
    expect(progressText({ phase: 'compacting', tool: null })).toBe('Trabajando…')
  })
  it('ni con nombres de Object.prototype (constructor, toString, __proto__)', () => {
    for (const phase of ['constructor', 'toString', '__proto__', 'hasOwnProperty']) {
      expect(progressText({ phase, tool: null })).toBe('Trabajando…')
    }
    expect(endReasonText({ reason: 'constructor' })).toBe('Falló.')
  })
})

describe('endReasonText', () => {
  it('no dice nada si terminó bien', () => {
    expect(endReasonText({ reason: 'done' })).toBeNull()
  })
  it('cuenta cada motivo, con el error de turno si lo hubo', () => {
    expect(endReasonText({ reason: 'cancelled' })).toBe('Se paró.')
    expect(endReasonText({ reason: 'interrupted' })).toMatch(/interrumpió/)
    expect(endReasonText({ reason: 'timeout' })).toMatch(/dejó de responder/)
    expect(endReasonText({ reason: 'error', error: 'orchestrator_unavailable' })).toBe('Falló: openclaw no está disponible.')
    expect(endReasonText({ reason: 'error', error: 'codigo_nuevo' })).toBe('Falló: openclaw rechazó la pregunta.')
  })
  it('un motivo que la web no conoce es un error', () => {
    expect(endReasonText({ reason: 'algo_nuevo' })).toBe('Falló.')
  })
})

describe('closeAction', () => {
  it('el 4102 retirado (5.0) de un gateway viejo cae en la regla de su rango: no reconecta solo', () => {
    expect(closeAction(4102)).toBe('stop')
  })
  it('para con 4000–4899 (también los que no conoce) y 1008, sin mirar el texto', () => {
    for (const code of [4000, 4001, 4002, 4010, 4020, 4021, 4100, 4101, 4200, 4350, 4899, 1008]) {
      expect(closeAction(code)).toBe('stop')
    }
    expect(stopText(4100)).toMatch(/ya no existe/)
    expect(stopText(4010)).toMatch(/ya no vale/)
    expect(stopText(4101)).toMatch(/no coincide/)
    expect(stopText(4321)).toBe('El gateway no aceptó la conexión del chat.')
  })
  it('reintenta tras caídas, fallos, reinicios, lentitud y un handshake a destiempo', () => {
    for (const code of [1006, 1011, 1012, 1013, 4900, 4950]) expect(closeAction(code)).toBe('retry')
  })
  it('con el acceso retirado (4200) para y lo dice: lo decide la persona', () => {
    expect(closeAction(4200)).toBe('stop')
    expect(stopText(4200)).toMatch(/Se retiró el acceso de este navegador/)
  })
  it('un cierre normal (inactividad) espera a que escribas', () => {
    expect(closeAction(1000)).toBe('idle')
  })
  it('espera cada vez más, hasta 30 s', () => {
    expect([1, 2, 3, 10].map(backoffMs)).toEqual([1000, 2000, 4000, 30_000])
  })
})
