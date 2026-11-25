import { describe, expect, it } from 'vitest'
import { backoffMs, ChatLog, closeAction, endReasonText, progressText, stopText } from './chat'

const start = (turn_id: string, extra: object = {}) => ({ type: 'turn_start', turn_id, turn_seq: 1, conversation: 'c1', origin: 'user', ...extra })

describe('ChatLog', () => {
  it('junta los tokens de un turno y lo cierra con su turn_end', () => {
    const log = new ChatLog('c1')
    log.addUser('¿Qué tiempo hace?')
    log.apply(start('t-1'))
    log.apply({ type: 'token', turn_id: 't-1', text: 'Sol' })
    expect(log.turnOpen).toBe(true)
    log.apply({ type: 'token', turn_id: 't-1', text: ' y 20 grados.' })
    log.apply({ type: 'turn_end', turn_id: 't-1', reason: 'done' })
    expect(log.turnOpen).toBe(false)
    expect(log.lines.map((l) => [l.role, l.text, l.state, l.reason])).toEqual([
      ['user', '¿Qué tiempo hace?', 'ended', 'done'],
      ['assistant', 'Sol y 20 grados.', 'ended', 'done'],
    ])
  })

  it('guarda por qué terminó un turno: cancelado, sustituido, timeout tras su error', () => {
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

  it('no acepta tokens de un turno ya cerrado ni de uno desconocido', () => {
    const log = new ChatLog('c1')
    log.apply(start('t-1'))
    log.apply({ type: 'turn_end', turn_id: 't-1', reason: 'replaced' })
    expect(log.apply({ type: 'token', turn_id: 't-1', text: 'tarde' })).toBe(false)
    expect(log.apply({ type: 'token', turn_id: 't-9', text: 'nadie' })).toBe(false)
    expect(log.lines[0]!.text).toBe('')
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

  it('tras reconectar, un turn_id repetido es un turno nuevo', () => {
    const log = new ChatLog('c1')
    log.apply(start('t-1'))
    log.apply({ type: 'token', turn_id: 't-1', text: 'uno' })
    log.closeOpenTurns() // se cayó el socket sin turn_end
    log.apply(start('t-1'))
    log.apply({ type: 'token', turn_id: 't-1', text: 'dos' })
    expect(log.lines.map((l) => [l.text, l.state, l.reason])).toEqual([
      ['uno', 'ended', 'closed'],
      ['dos', 'streaming', undefined],
    ])
    expect(new Set(log.lines.map((l) => l.key)).size).toBe(2)
  })
})

describe('ChatLog al volver a leer lo dicho', () => {
  const end = (turn_id: string, reason = 'done') => ({ type: 'turn_end', turn_id, reason })
  const fail = (turn_id: string, code: string) => ({ type: 'error', code, message: code, fatal: false, turn_id })
  const texts = (log: ChatLog) => log.lines.map((l) => l.text)

  it('quita lo terminado antes de pedir y deja lo que sigue llegando, para no enseñar nada dos veces', () => {
    const log = new ChatLog('c1')
    log.addUser('uno')
    log.apply(start('t-1'))
    log.apply(end('t-1'))
    log.addUser('dos')
    log.apply(start('t-2'))
    log.dropReloaded(log.endedKeys())
    expect(log.lines.map((l) => [l.role, l.state])).toEqual([['assistant', 'streaming']])
    log.apply({ type: 'token', turn_id: 't-2', text: 'sigue' })
    expect(log.lines[0]!.text).toBe('sigue')
  })

  it('lo que termina mientras llega la lectura se queda: no venía en ella', () => {
    const log = new ChatLog('c1')
    log.addUser('uno')
    log.apply(start('t-1'))
    const keys = log.endedKeys()
    log.apply({ type: 'token', turn_id: 't-1', text: 'tarde' })
    log.apply(end('t-1'))
    log.addUser('dos')
    log.dropReloaded(keys)
    expect(texts(log)).toEqual(['tarde', 'dos'])
  })

  it('se queda el push de otra conversación y lo que no salió', () => {
    const log = new ChatLog('c1')
    log.apply(start('t-1', { conversation: 'c2', origin: 'push' }))
    log.apply({ type: 'token', turn_id: 't-1', text: 'de otra' })
    log.apply(end('t-1'))
    log.markUnsent(log.addUser('no salió'))
    log.dropReloaded(log.endedKeys())
    expect(texts(log)).toEqual(['de otra', 'no salió'])
  })

  it('se queda lo que espera a salir (`keep`)', () => {
    const log = new ChatLog('c1')
    log.addUser('ya dicho')
    const pending = log.addUser('esperando')
    log.dropReloaded(log.endedKeys(), pending)
    expect(texts(log)).toEqual(['esperando'])
  })

  it('se queda el turno que no llegó a OpenClaw con tu mensaje; los demás errores ya están en OpenClaw', () => {
    const log = new ChatLog('c1')
    const turns = [
      'orchestrator_unavailable',
      'orchestrator_send_failed',
      'orchestrator_agent_unavailable',
      'orchestrator_error',
      'orchestrator_auth_failed',
      'turn_timeout',
    ]
    turns.forEach((code, i) => {
      log.sent(log.addUser(`pregunta ${code}`))
      log.apply(start(`t-${i}`))
      log.apply(fail(`t-${i}`, code))
      log.apply(end(`t-${i}`, 'error'))
    })
    log.sent(log.addUser('cancelada'))
    log.apply(start('t-9'))
    log.apply(end('t-9', 'cancelled'))
    log.dropReloaded(log.endedKeys())
    expect(log.lines.map((l) => [l.role, l.text, l.error])).toEqual([
      ['user', 'pregunta orchestrator_unavailable', undefined],
      ['assistant', '', 'orchestrator_unavailable'],
      ['user', 'pregunta orchestrator_send_failed', undefined],
      ['assistant', '', 'orchestrator_send_failed'],
      ['user', 'pregunta orchestrator_agent_unavailable', undefined],
      ['assistant', '', 'orchestrator_agent_unavailable'],
    ])
  })

  it('cada respuesta va con tu mensaje más viejo sin responder, aunque un push se cuele en medio', () => {
    const log = new ChatLog('c1')
    const u1 = log.addUser('uno')
    log.sent(u1)
    const u2 = log.addUser('dos')
    log.sent(u2)
    log.apply(start('t-1', { origin: 'push' }))
    log.apply(start('t-2'))
    log.apply(start('t-3'))
    expect(log.lines.map((l) => l.ask)).toEqual([undefined, undefined, undefined, u1, u2])
  })

  it('el turno de un mensaje de otro aparato no responde a lo tuyo (5.0: turn_start.by)', () => {
    const log = new ChatLog('c1')
    const u1 = log.addUser('uno')
    log.sent(u1)
    log.apply(start('t-1', { by: { device_id: 'tele', name: 'Tele', self: false } }))
    log.apply(start('t-2', { by: { device_id: 'web', name: 'Web', self: true } }))
    expect(log.lines.map((l) => l.ask)).toEqual([undefined, undefined, u1])
  })

  it('lo enviado sin turn_start todavía no cuenta como leído: OpenClaw quizá aún no lo tiene', () => {
    const log = new ChatLog('c1')
    log.sent(log.addUser('recién enviado'))
    const keys = log.endedKeys()
    expect(keys).toEqual([])
    log.dropReloaded([...keys, 'u-1']) // aunque viniera en una lectura anterior
    expect(texts(log)).toEqual(['recién enviado'])
  })

  it('al cerrarse el socket, lo enviado sin turn_start queda «no enviado»', () => {
    const log = new ChatLog('c1')
    log.sent(log.addUser('respondido'))
    log.apply(start('t-1'))
    log.sent(log.addUser('perdido'))
    log.closeOpenTurns()
    expect(log.lines.map((l) => [l.text, l.unsent])).toEqual([
      ['respondido', undefined],
      ['', undefined],
      ['perdido', true],
    ])
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
    expect(endReasonText({ reason: 'cancelled' })).toBe('La paraste aquí.')
    expect(endReasonText({ reason: 'interrupted' })).toMatch(/interrumpió/)
    expect(endReasonText({ reason: 'timeout' })).toMatch(/dejó de responder/)
    expect(endReasonText({ reason: 'error', error: 'orchestrator_unavailable' })).toBe('Falló: openclaw no está disponible.')
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
