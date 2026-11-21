import { beforeEach, describe, expect, it } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { useSessionStore } from './session'

const SITO = { id: 'u1', username: 'sito', display_name: 'Sito', role: 'owner' as const }

describe('sesión (4.0)', () => {
  beforeEach(() => {
    localStorage.clear()
    sessionStorage.clear()
    setActivePinia(createPinia())
  })

  it('guarda token, dispositivo y persona, y sabe si cuida del hogar', () => {
    const s = useSessionStore()
    s.signIn('fgr_tok', 'dev-1', SITO, true)
    expect(s.isAdmin).toBe(true)
    setActivePinia(createPinia())
    const otra = useSessionStore()
    expect([otra.token, otra.deviceId, otra.user?.username]).toEqual(['fgr_tok', 'dev-1', 'sito'])
    otra.setUser({ ...SITO, role: 'member' })
    expect(otra.isAdmin).toBe(false)
    otra.signOut()
    expect(localStorage.length + sessionStorage.length).toBe(0)
  })

  it('sin recordar, solo en esta pestaña; el token de la 3.x se olvida', () => {
    localStorage.setItem('fogar.adminToken', 'viejo')
    const s = useSessionStore()
    expect(s.token).toBeNull()
    expect(localStorage.getItem('fogar.adminToken')).toBeNull()
    s.signIn('fgr_tok', 'dev-1', SITO, false)
    expect(localStorage.getItem('fogar.session')).toBeNull()
    expect(sessionStorage.getItem('fogar.session')).not.toBeNull()
  })
})
