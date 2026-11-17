import { createPinia } from 'pinia'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createApp, defineComponent, nextTick } from 'vue'
import { createMemoryHistory, createRouter, RouterView } from 'vue-router'
import { ApiError } from '@/api/http'
import type { ClientResponse, ClientWithKey } from '@/api/types'
import { clearToasts, toasts } from '@/lib/toasts'
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
const deleteClient = vi.mocked(admin.deleteClient)

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
      { path: '/clientes', component: defineComponent({ render: () => null }) },
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
  vi.clearAllMocks()
  clearToasts()
  getClient.mockResolvedValue(cliente)
})

afterEach(() => vi.useRealTimers())

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

describe('ClienteView: acciones que no se repiten ni engañan (G-r6-2..4)', () => {
  it('un doble clic en «Rotar ahora» rota una sola vez', async () => {
    rotateClientKey.mockReturnValue(new Promise(() => undefined))
    const { boton } = await mount()
    boton('Rotar clave').click()
    await flush()
    boton('Rotar ahora').click()
    await flush()
    boton('Rotar ahora').click()
    await flush()
    expect(rotateClientKey).toHaveBeenCalledTimes(1)
  })

  it('sin respuesta al rotar relee la huella antes de enseñarla (G-r7-1)', async () => {
    rotateClientKey.mockRejectedValue(new ApiError('network', 0, 'No llego al gateway'))
    const { el, boton } = await mount()
    vi.useFakeTimers()
    // la primera lectura se adelanta a la rotación; la segunda ya la ve. Otro nombre: solo se toma la huella
    getClient.mockResolvedValueOnce(cliente).mockResolvedValue({ ...cliente, name: 'Renombrado', key_fp: 'ffff0000' })
    boton('Rotar clave').click()
    await flush()
    boton('Rotar ahora').click()
    await flush()
    expect(toasts.map((t) => t.title)).toEqual(['No sé si se rotó la clave'])
    expect(el.textContent).toContain('huella sin comprobar')
    expect(el.textContent).not.toContain('abcd')
    await vi.advanceTimersByTimeAsync(3000)
    vi.useRealTimers()
    await flush()
    expect(toasts[0]!.title).toBe('Sí se rotó la clave')
    expect(el.textContent).toContain('ffff')
    expect(el.querySelector('h1')!.textContent).toBe('Tele')
  })

  it('una lectura que se adelantó y luego fallos no cuentan como comprobada (G-r7-1)', async () => {
    rotateClientKey.mockRejectedValue(new ApiError('network', 0, 'No llego al gateway'))
    const { el, boton } = await mount()
    vi.useFakeTimers()
    getClient.mockResolvedValueOnce(cliente).mockRejectedValue(new ApiError('network', 0, 'No llego al gateway'))
    boton('Rotar clave').click()
    await flush()
    boton('Rotar ahora').click()
    await vi.advanceTimersByTimeAsync(3000)
    vi.useRealTimers()
    await flush()
    expect(toasts[0]!.title).toBe('No he podido comprobarlo')
    expect(el.textContent).toContain('huella sin comprobar')
  })

  it('si no puede releer la huella, no enseña la vieja y pide recargar (G-r7-1)', async () => {
    rotateClientKey.mockRejectedValue(new ApiError('network', 0, 'No llego al gateway'))
    const { el, boton } = await mount()
    vi.useFakeTimers()
    getClient.mockRejectedValue(new ApiError('network', 0, 'No llego al gateway'))
    boton('Rotar clave').click()
    await flush()
    boton('Rotar ahora').click()
    await vi.advanceTimersByTimeAsync(3000)
    vi.useRealTimers()
    await flush()
    expect(getClient).toHaveBeenCalledTimes(4) // la carga y 3 intentos
    expect(toasts[0]!.title).toBe('No he podido comprobarlo')
    expect(toasts[0]!.body).toMatch(/Recarga la página/)
    expect(el.textContent).toContain('huella sin comprobar')
    expect(el.textContent).not.toContain('abcd')
  })

  it('con error HTTP al rotar, la clave de antes sigue valiendo', async () => {
    rotateClientKey.mockRejectedValue(new ApiError('server', 500, 'x'))
    const { boton } = await mount()
    boton('Rotar clave').click()
    await flush()
    boton('Rotar ahora').click()
    await flush()
    expect(toasts.map((t) => t.body)).toEqual(['La clave de antes sigue valiendo.'])
  })

  it('borrar con cambios sin guardar sale sin preguntar', async () => {
    deleteClient.mockResolvedValue(undefined)
    const { el, router, boton } = await mount()
    const nombre = el.querySelector('form input') as HTMLInputElement
    nombre.value = 'Otro nombre'
    nombre.dispatchEvent(new Event('input'))
    await flush()
    boton('Borrar…').click()
    await flush()
    const confirmar = el.querySelector('form.confirmar input') as HTMLInputElement
    confirmar.value = 'Tele'
    confirmar.dispatchEvent(new Event('input'))
    await flush()
    const confirm = vi.spyOn(window, 'confirm').mockReturnValue(false)
    el.querySelector('form.confirmar')!.dispatchEvent(new Event('submit'))
    await flush()
    await new Promise((r) => setTimeout(r, 0)) // la navegación resuelve fuera de los ticks de Vue
    expect(deleteClient).toHaveBeenCalled()
    expect(confirm).not.toHaveBeenCalled()
    expect(router.currentRoute.value.path).toBe('/clientes')
  })

  it('un 404 al borrar es que ya estaba borrado: sale a la lista (G-r7-4)', async () => {
    deleteClient.mockRejectedValue(new ApiError('not-found', 404, 'x'))
    const { el, router, boton } = await mount()
    boton('Borrar…').click()
    await flush()
    const confirmar = el.querySelector('form.confirmar input') as HTMLInputElement
    confirmar.value = 'Tele'
    confirmar.dispatchEvent(new Event('input'))
    await flush()
    el.querySelector('form.confirmar')!.dispatchEvent(new Event('submit'))
    await flush()
    await new Promise((r) => setTimeout(r, 0))
    expect(toasts.map((t) => t.title)).toEqual(['«Tele» borrado'])
    expect(router.currentRoute.value.path).toBe('/clientes')
  })
})
