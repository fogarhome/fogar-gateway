import { afterEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { createConversation, getConversationAgents, getConversationMessages, listConversations, postMessage } from './admin'

function reply(status: number, body: unknown) {
  const fetch = vi.fn(async (_url: string, _init?: RequestInit) => new Response(JSON.stringify(body), { status }))
  vi.stubGlobal('fetch', fetch)
  return fetch
}

describe('conversaciones (M19)', () => {
  setActivePinia(createPinia())
  afterEach(() => vi.unstubAllGlobals())

  it('lista y lee lo dicho de una, con su límite', async () => {
    let fetch = reply(200, [])
    await listConversations()
    expect(fetch.mock.calls[0]![0]).toBe('/conversations')
    fetch = reply(200, { conversation_id: 'a b', messages: [], has_more: false })
    await getConversationMessages('a b', 1000)
    expect(fetch.mock.calls[0]![0]).toBe('/conversations/a%20b/messages?limit=1000')
  })

  it('crea con el agente elegido, o sin agente para el de por defecto', async () => {
    const fetch = reply(201, { id: 'n' })
    await createConversation('casa')
    await createConversation(null)
    expect(fetch.mock.calls.map(([, init]) => [init?.method, init?.body])).toEqual([
      ['POST', '{"agent":"casa"}'],
      ['POST', '{}'],
    ])
  })

  it('enviar espera hasta 130 s su 202: la conversación puede estar ocupada hasta TURN_TIMEOUT_S', async () => {
    reply(202, { message_id: 'm1' })
    const timeout = vi.spyOn(AbortSignal, 'timeout')
    await postMessage('c1', 'hola')
    expect(timeout).toHaveBeenCalledWith(130_000)
    timeout.mockRestore()
  })

  it('los errores llegan con su código estable', async () => {
    reply(503, { detail: 'orchestrator_unavailable' })
    await expect(getConversationMessages('x')).rejects.toMatchObject({ status: 503, message: 'orchestrator_unavailable' })
    reply(401, { detail: 'not_authenticated' })
    await expect(getConversationAgents()).rejects.toMatchObject({ kind: 'unauthorized', message: 'not_authenticated' })
  })
})
