import { createPinia } from 'pinia'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createApp, defineComponent, nextTick } from 'vue'
import { createMemoryHistory, createRouter, RouterView } from 'vue-router'
import { ApiError } from '@/api/http'
import type { ClientWithKey } from '@/api/types'
import ClientesView from './ClientesView.vue'

vi.mock('@/api/admin', () => ({
  createClient: vi.fn(),
  listClients: vi.fn(() => Promise.resolve([])),
}))
const admin = await import('@/api/admin')
const createClient = vi.mocked(admin.createClient)
const listClients = vi.mocked(admin.listClients)

const nuevo: ClientWithKey = {
  id: 'c1',
  name: 'Tele',
  client_key: 'k'.repeat(40),
  key_fp: 'abcd1234',
  is_active: true,
  created_at: '2026-11-17T10:00:00Z',
} as ClientWithKey

async function mount() {
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: '/clientes', component: ClientesView },
      { path: '/otra', component: defineComponent({ render: () => null }) },
    ],
  })
  await router.push('/clientes')
  const el = document.createElement('div')
  createApp(RouterView).use(createPinia()).use(router).mount(el)
  await flush()
  const boton = (texto: string) =>
    [...el.querySelectorAll('button')].find((b) => b.textContent?.includes(texto)) as HTMLButtonElement
  return { el, router, boton }
}
const flush = async () => {
  for (let i = 0; i < 5; i++) await nextTick()
}

beforeEach(() => {
  vi.clearAllMocks()
  listClients.mockResolvedValue([])
})

afterEach(() => vi.useRealTimers())

describe('ClientesView: la clave nueva no se pierde', () => {
  it('mientras se crea, «Cancelar» y las cabeceras están desactivados y la clave sale al terminar', async () => {
    let resolver!: (c: ClientWithKey) => void
    createClient.mockReturnValue(new Promise((r) => (resolver = r)))
    const { el, boton } = await mount()
    boton('Crear a mano').click()
    await flush()
    const nombre = el.querySelector('form input') as HTMLInputElement
    nombre.value = 'Tele'
    nombre.dispatchEvent(new Event('input'))
    el.querySelector('form')!.dispatchEvent(new Event('submit'))
    await flush()
    expect(boton('Cancelar').disabled).toBe(true)
    expect(boton('Crear a mano').disabled).toBe(true)
    expect(boton('Emparejar un aparato').disabled).toBe(true)
    resolver(nuevo)
    await flush()
    expect((el.querySelector('input[readonly]') as HTMLInputElement).value).toBe(nuevo.client_key)
  })

  it('salir con la clave a la vista pide confirmación', async () => {
    createClient.mockResolvedValue(nuevo)
    const { el, router, boton } = await mount()
    boton('Crear a mano').click()
    await flush()
    const nombre = el.querySelector('form input') as HTMLInputElement
    nombre.value = 'Tele'
    nombre.dispatchEvent(new Event('input'))
    el.querySelector('form')!.dispatchEvent(new Event('submit'))
    await flush()
    const confirm = vi.spyOn(window, 'confirm').mockReturnValue(false)
    await router.push('/otra')
    expect(confirm).toHaveBeenCalled()
    expect(router.currentRoute.value.path).toBe('/clientes')
  })

  it('salir o recargar con el POST de crear en curso pide confirmación', async () => {
    createClient.mockReturnValue(new Promise(() => undefined))
    const { el, router, boton } = await mount()
    boton('Crear a mano').click()
    await flush()
    const nombre = el.querySelector('form input') as HTMLInputElement
    nombre.value = 'Tele'
    nombre.dispatchEvent(new Event('input'))
    el.querySelector('form')!.dispatchEvent(new Event('submit'))
    await flush()
    const recarga = new Event('beforeunload', { cancelable: true })
    window.dispatchEvent(recarga)
    expect(recarga.defaultPrevented).toBe(true)
    const confirm = vi.spyOn(window, 'confirm').mockReturnValue(false)
    await router.push('/otra')
    expect(confirm).toHaveBeenCalled()
    expect(router.currentRoute.value.path).toBe('/clientes')
  })
})

describe('ClientesView: crear sin respuesta (G-r6-3)', () => {
  it('con error de red no invita a reintentar a ciegas y recarga la lista', async () => {
    createClient.mockRejectedValue(new ApiError('network', 0, 'No llego al gateway'))
    const { el, boton } = await mount()
    vi.useFakeTimers()
    boton('Crear a mano').click()
    await flush()
    const nombre = el.querySelector('form input') as HTMLInputElement
    nombre.value = 'Tele'
    nombre.dispatchEvent(new Event('input'))
    listClients.mockClear()
    el.querySelector('form')!.dispatchEvent(new Event('submit'))
    await flush()
    expect(el.textContent).toContain('No sé si se creó')
    await vi.advanceTimersByTimeAsync(1000)
    vi.useRealTimers()
    await flush()
    expect(listClients).toHaveBeenCalled()
    expect(el.textContent).toContain('su clave no se ha visto: entra en él y rota la clave')
  })

  it('mientras relee la lista no deja crear otra vez (G-r7-1)', async () => {
    createClient.mockRejectedValue(new ApiError('network', 0, 'No llego al gateway'))
    const { el, boton } = await mount()
    vi.useFakeTimers()
    boton('Crear a mano').click()
    await flush()
    const nombre = el.querySelector('form input') as HTMLInputElement
    nombre.value = 'Tele'
    nombre.dispatchEvent(new Event('input'))
    el.querySelector('form')!.dispatchEvent(new Event('submit'))
    await flush()
    expect(boton('Crear y ver la clave').disabled).toBe(true)
    el.querySelector('form')!.dispatchEvent(new Event('submit'))
    await flush()
    expect(createClient).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(1000)
    vi.useRealTimers()
    await flush()
    expect(boton('Crear y ver la clave').disabled).toBe(false)
  })

  it('si no puede releer la lista, pide recargar antes de reintentar (G-r7-1)', async () => {
    createClient.mockRejectedValue(new ApiError('network', 0, 'No llego al gateway'))
    const { el, boton } = await mount()
    vi.useFakeTimers()
    boton('Crear a mano').click()
    await flush()
    const nombre = el.querySelector('form input') as HTMLInputElement
    nombre.value = 'Tele'
    nombre.dispatchEvent(new Event('input'))
    listClients.mockRejectedValue(new Error('No llego al gateway'))
    el.querySelector('form')!.dispatchEvent(new Event('submit'))
    await vi.advanceTimersByTimeAsync(3000)
    vi.useRealTimers()
    await flush()
    expect(listClients).toHaveBeenCalledTimes(4) // al montar y 3 intentos
    expect(el.textContent).toContain('no he podido comprobarlo: recarga la página')
    expect(el.textContent).toContain('«Tele» sale en la lista, su clave no se ha visto')
  })
})
