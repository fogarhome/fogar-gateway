import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { useEventsStore } from './events'
import { useSessionStore } from './session'

const SITO = { id: 'u1', username: 'sito', display_name: 'Sito', role: 'owner' as const }

describe('el /events de la pestaña', () => {
  beforeEach(() => {
    localStorage.clear()
    sessionStorage.clear()
    setActivePinia(createPinia())
    useSessionStore().signIn('fgr_tok', 'dev-1', SITO, false)
  })
  afterEach(() => vi.unstubAllGlobals())

  it('parado (401), «Reintentar» (ensure) lo vuelve a abrir de verdad', async () => {
    const fetch = vi.fn(async () => new Response(null, { status: 401 }))
    vi.stubGlobal('fetch', fetch)
    const events = useEventsStore()
    events.ensure()
    await vi.waitFor(() => expect(events.connection).toBe('stopped'))
    events.ensure()
    await vi.waitFor(() => expect(fetch).toHaveBeenCalledTimes(2))
    events.stop()
  })
})
