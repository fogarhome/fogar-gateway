import { describe, expect, it } from 'vitest'
import { canWrite, conversationKind, conversationScope, conversationTitle } from './conversations'

const now = new Date(2026, 10, 14, 12)
const base = { title: null, kind: 'created', day: null, created_at: new Date(2026, 10, 12, 9).toISOString() }

describe('conversationTitle', () => {
  it('usa el título de OpenClaw', () => {
    expect(conversationTitle({ ...base, title: '  La cena del sábado ' }, now)).toBe('La cena del sábado')
  })
  it('sin título, la del día lleva su día', () => {
    expect(conversationTitle({ ...base, kind: 'default', day: '2026-11-14' }, now)).toBe('Conversación del 14 nov')
  })
  it('sin título, las demás lo dicen y llevan cuándo se crearon', () => {
    expect(conversationTitle(base, now)).toBe('Sin título · 12 nov')
    expect(conversationTitle({ ...base, title: '   ' }, now)).toBe('Sin título · 12 nov')
  })
})

describe('conversationKind y canWrite', () => {
  it('solo nombra la clase cuando aporta', () => {
    expect([conversationKind('default'), conversationKind('legacy'), conversationKind('created')]).toEqual(['diaria', 'de antes', null])
  })
  it('el panel escribe solo en las suyas', () => {
    expect(canWrite({ client_id: 'mi-navegador' }, 'mi-navegador')).toBe(true)
    expect(canWrite({ client_id: 'tele-salon' }, 'mi-navegador')).toBe(false)
    expect(canWrite({ client_id: 'mi-navegador' }, null)).toBe(false)
    expect([conversationScope({ owner_user_id: 'u1' }), conversationScope({ owner_user_id: null })]).toEqual(['privada', 'del hogar'])
  })
})
