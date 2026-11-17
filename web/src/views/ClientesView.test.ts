import { createPinia } from 'pinia'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { createApp, defineComponent, nextTick } from 'vue'
import { createMemoryHistory, createRouter, RouterView } from 'vue-router'
import type { ClientWithKey } from '@/api/types'
import ClientesView from './ClientesView.vue'

vi.mock('@/api/admin', () => ({
  createClient: vi.fn(),
  listClients: vi.fn(() => Promise.resolve([])),
}))
const createClient = vi.mocked((await import('@/api/admin')).createClient)

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

beforeEach(() => vi.clearAllMocks())

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
})
