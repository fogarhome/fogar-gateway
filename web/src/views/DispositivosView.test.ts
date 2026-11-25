import { createPinia } from 'pinia'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { createApp, nextTick } from 'vue'
import { createMemoryHistory, createRouter, RouterView } from 'vue-router'
import type { Me, MyDevice } from '@/api/types'
import DispositivosView from './DispositivosView.vue'

vi.mock('@/api/admin', () => ({
  getMe: vi.fn(),
  updateMe: vi.fn(),
  listMyDevices: vi.fn(),
  retireMyDevice: vi.fn(),
}))
vi.mock('@/lib/toasts', () => ({ toast: vi.fn() }))
const admin = vi.mocked(await import('@/api/admin'))

const device = (id: string, name: string, extra: Partial<MyDevice> = {}): MyDevice => ({
  id,
  name,
  client_type: 'app',
  created_at: '2026-11-20T10:00:00Z',
  last_seen_at: null,
  is_active: true,
  current: false,
  ...extra,
})
const me = (notify: string | null): Me => ({
  user: { id: 'u1', username: 'ana', display_name: 'Ana', role: 'member' },
  home: { id: 'h', name: 'Casa' },
  device: null,
  notify_client_id: notify,
})

async function mount() {
  const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/', component: DispositivosView }] })
  await router.push('/')
  const el = document.createElement('div')
  createApp(RouterView).use(createPinia()).use(router).mount(el)
  await flush()
  return { el, select: el.querySelector('select') as HTMLSelectElement }
}
const flush = async () => {
  for (let i = 0; i < 8; i++) await nextTick()
}

beforeEach(() => {
  vi.clearAllMocks()
  admin.listMyDevices.mockResolvedValue([
    device('d1', 'Móvil'),
    device('d2', 'Este navegador', { current: true }),
    device('d3', 'Viejo', { is_active: false }),
    device('d4', 'Mudo', { push_enabled: false }),
    device('d5', 'Portátil', { client_type: 'web', output_mode: null }),
  ])
})

describe('DispositivosView: «Avisos en…»', () => {
  it('ofrece mis aparatos activos y «Ninguno», y avisa si no hay uno elegido', async () => {
    admin.getMe.mockResolvedValue(me(null))
    const { el, select } = await mount()
    expect(el.textContent).toContain('Avisos en…')
    expect([...select.options].map((o) => o.textContent)).toEqual(['Ninguno', 'Móvil', 'Este navegador (este)', 'Portátil (solo texto, no suena)'])
    expect(el.textContent).toContain('Elige dónde suenan los avisos')
  })

  it('elegir uno lo guarda con PATCH /auth/me y quita el aviso', async () => {
    admin.getMe.mockResolvedValue(me(null))
    admin.updateMe.mockResolvedValue(me('d1'))
    const { el, select } = await mount()
    select.value = 'd1'
    select.dispatchEvent(new Event('change'))
    await flush()
    expect(admin.updateMe).toHaveBeenCalledWith({ notify_client_id: 'd1' })
    expect(el.textContent).not.toContain('Elige dónde suenan los avisos')
  })
})
