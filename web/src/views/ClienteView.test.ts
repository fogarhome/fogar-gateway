import { createPinia } from 'pinia'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { createApp, defineComponent, nextTick } from 'vue'
import { createMemoryHistory, createRouter, RouterView } from 'vue-router'
import type { ClientResponse, ClientWithKey } from '@/api/types'
import ClienteView from './ClienteView.vue'

vi.mock('@/api/admin', () => ({
  getClient: vi.fn(),
  rotateClientKey: vi.fn(),
  updateClient: vi.fn(),
  deleteClient: vi.fn(),
  listClients: vi.fn(() => Promise.resolve([])),
}))
const admin = await import('@/api/admin')
const getClient = vi.mocked(admin.getClient)
const rotateClientKey = vi.mocked(admin.rotateClientKey)

const cliente: ClientResponse = {
  id: 'c1',
  name: 'Tele',
  key_fp: 'abcd1234',
  is_active: true,
  client_type: null,
  default_agent: null,
  allowed_agents: null,
  created_at: '2026-11-17T10:00:00Z',
  stt_language: 'es',
  stt_vad_thold: 0.5,
  tts_voice: 'v',
  tts_speed: 1,
  barge_in_enabled: true,
  barge_in_min_chars: 3,
  output_mode: null,
  silence_timeout_s: 10,
  max_silence_turns: 2,
  push_enabled: false,
  tool_calls_enabled: false,
}

const flush = async () => {
  for (let i = 0; i < 5; i++) await nextTick()
}
async function mount() {
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: '/clientes/:id', component: ClienteView },
      { path: '/otra', component: defineComponent({ render: () => null }) },
    ],
  })
  await router.push('/clientes/c1')
  const el = document.createElement('div')
  createApp(RouterView).use(createPinia()).use(router).mount(el)
  await flush()
  const boton = (texto: string) =>
    [...el.querySelectorAll('button')].find((b) => b.textContent?.includes(texto)) as HTMLButtonElement
  return { el, router, boton }
}

beforeEach(() => {
  vi.restoreAllMocks()
  getClient.mockResolvedValue(cliente)
})

describe('ClienteView: rotar la clave', () => {
  it('salir o recargar con el POST de rotar en curso pide confirmación', async () => {
    rotateClientKey.mockReturnValue(new Promise(() => undefined))
    const { router, boton } = await mount()
    boton('Rotar clave').click()
    await flush()
    boton('Rotar ahora').click()
    await flush()
    const recarga = new Event('beforeunload', { cancelable: true })
    window.dispatchEvent(recarga)
    expect(recarga.defaultPrevented).toBe(true)
    const confirm = vi.spyOn(window, 'confirm').mockReturnValue(false)
    await router.push('/otra')
    expect(confirm).toHaveBeenCalled()
    expect(router.currentRoute.value.path).toBe('/clientes/c1')
  })

  it('la clave rotada no se queda en el cliente', async () => {
    const r: ClientWithKey = { ...cliente, key_fp: 'ffff0000', client_key: 'k'.repeat(40) }
    rotateClientKey.mockResolvedValue(r)
    const { el, boton } = await mount()
    boton('Rotar clave').click()
    await flush()
    boton('Rotar ahora').click()
    await flush()
    expect((el.querySelector('input[readonly]') as HTMLInputElement).value).toBe(r.client_key)
    expect(el.textContent).toContain('ffff')
  })
})
