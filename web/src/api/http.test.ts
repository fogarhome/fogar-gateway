import { afterEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { api, ApiError } from './http'

function reply(status: number, body: unknown): void {
  vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify(body), { status })))
}

describe('api', () => {
  setActivePinia(createPinia())
  afterEach(() => vi.unstubAllGlobals())

  it('un 503 de /admin es «not-configured»', async () => {
    reply(503, { detail: 'admin disabled' })
    await expect(api('/system/state')).rejects.toMatchObject({ kind: 'not-configured', status: 503 })
  })

  it('/ready con OpenClaw caído responde 503 con cuerpo: se lee, no es un error', async () => {
    const body = { status: 'unavailable', services: { orchestrator: 'unavailable', transcriber: 'ok', tts: 'ok' } }
    reply(503, body)
    await expect(api('/ready', { okStatuses: [503] })).resolves.toEqual(body)
  })

  it('manda el token de la sesión como Bearer; con token: null, ninguno', async () => {
    const fetch = vi.fn(async () => new Response('{}', { status: 200 }))
    vi.stubGlobal('fetch', fetch)
    const { useSessionStore } = await import('@/stores/session')
    useSessionStore().signIn('fgr_tok', 'dev-1', { id: 'u', username: 'u', display_name: 'U', role: 'member' }, false)
    await api('/auth/me')
    await api('/auth/login', { token: null, method: 'POST', body: '{}' })
    const headers = fetch.mock.calls.map((c) => (c as unknown as [string, RequestInit])[1].headers as Record<string, string>)
    expect(headers[0]?.Authorization).toBe('Bearer fgr_tok')
    expect(headers[1]?.Authorization).toBeUndefined()
  })

  it('un 403 es «forbidden» y un 429 «too-many»', async () => {
    reply(403, { detail: 'forbidden' })
    await expect(api('/home/devices')).rejects.toMatchObject({ kind: 'forbidden', message: 'forbidden' })
    reply(429, { detail: 'too_many_attempts' })
    await expect(api('/auth/login')).rejects.toMatchObject({ kind: 'too-many' })
  })

  it('otros fallos siguen siendo errores aunque se acepte el 503', async () => {
    reply(500, {})
    await expect(api('/ready', { okStatuses: [503] })).rejects.toBeInstanceOf(ApiError)
  })
})
