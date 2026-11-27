import { describe, expect, it } from 'vitest'
import { conversationKind, conversationScope, conversationTitle, historyAuthor } from './conversations'

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

describe('conversationKind y conversationScope', () => {
  it('solo nombra la clase cuando aporta', () => {
    expect([conversationKind('default'), conversationKind('created')]).toEqual(['diaria', null])
  })
  it('dice de quién es', () => {
    expect([conversationScope({ owner_user_id: 'u1' }), conversationScope({ owner_user_id: null })]).toEqual(['privada', 'del hogar'])
  })
})

describe('historyAuthor', () => {
  it('en lo leído, lo mío en mi privada es «Tú», como en vivo; en una del hogar no se sabe de quién', () => {
    expect(historyAuthor('user', { owner_user_id: 'u1' }, 'u1')).toBe('Tú')
    expect(historyAuthor('user', { owner_user_id: null }, 'u1')).toBe('Persona')
    expect(historyAuthor('user', null, 'u1')).toBe('Persona')
    expect(historyAuthor('assistant', { owner_user_id: 'u1' }, 'u1')).toBe('Fogar')
  })
})
