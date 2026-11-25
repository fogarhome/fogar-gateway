import { createPinia } from 'pinia'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { createApp, nextTick } from 'vue'
import type { ClientResponse, Home, UserRow } from '@/api/types'
import PersonasView from './PersonasView.vue'

vi.mock('@/api/admin', () => ({
  createUser: vi.fn(),
  deleteUser: vi.fn(),
  updateUser: vi.fn(),
  listUsers: vi.fn(),
  getHome: vi.fn(),
  updateHome: vi.fn(),
  listClients: vi.fn(),
}))
vi.mock('@/lib/toasts', () => ({ toast: vi.fn() }))
const admin = vi.mocked(await import('@/api/admin'))

const user = (id: string, name: string, is_active = true): UserRow => ({
  id,
  username: id,
  display_name: name,
  role: 'member',
  agent: null,
  is_active,
  created_at: '2026-11-20T10:00:00Z',
  effective_agent: 'main',
  agent_shared: false,
})
const client = (id: string, name: string, extra: Partial<ClientResponse> = {}) =>
  ({ id, name, is_active: true, user_id: null, ...extra }) as ClientResponse
const home = (notify: string | null): Home => ({ id: 'h', name: 'Casa', agent: null, notify_client_id: notify })

async function mount() {
  const el = document.createElement('div')
  createApp(PersonasView).use(createPinia()).mount(el)
  await flush()
  const select = [...el.querySelectorAll('label')]
    .find((l) => l.textContent?.includes('Avisos del hogar en…'))
    ?.querySelector('select') as HTMLSelectElement
  return { el, select }
}
const flush = async () => {
  for (let i = 0; i < 8; i++) await nextTick()
}

beforeEach(() => {
  vi.clearAllMocks()
  admin.listUsers.mockResolvedValue([user('u1', 'Ana'), user('u2', 'Luis', false)])
  admin.listClients.mockResolvedValue([
    client('tv', 'Tele'),
    client('off', 'Altavoz', { is_active: false }),
    client('m1', 'Móvil', { user_id: 'u1' }),
    client('m2', 'Tablet', { user_id: 'u2' }),
    client('mudo', 'Radio', { push_enabled: false }),
    client('pantalla', 'Pantalla', { output_mode: ['text'] }),
  ])
})

describe('PersonasView: «Avisos del hogar en…»', () => {
  it('ofrece los aparatos activos del hogar (sin desactivados ni de personas desactivadas) y avisa si no hay', async () => {
    admin.getHome.mockResolvedValue(home(null))
    const { el, select } = await mount()
    expect([...select.options].map((o) => o.textContent)).toEqual(['Ninguno', 'Tele', 'Móvil · de Ana', 'Pantalla (solo texto, no suena)'])
    expect(el.textContent).toContain('Elige dónde suenan los avisos')
  })

  it('elegir uno lo guarda con PATCH /admin/home', async () => {
    admin.getHome.mockResolvedValue(home(null))
    admin.updateHome.mockResolvedValue(home('tv'))
    const { el, select } = await mount()
    select.value = 'tv'
    select.dispatchEvent(new Event('change'))
    await flush()
    expect(admin.updateHome).toHaveBeenCalledWith({ notify_client_id: 'tv' })
    expect(el.textContent).not.toContain('Elige dónde suenan los avisos')
  })

  it('si no puede leer los aparatos, Personas sigue y el selector dice por qué', async () => {
    admin.getHome.mockResolvedValue(home(null))
    admin.listClients.mockRejectedValue(new Error('caído'))
    const { el, select } = await mount()
    expect(select).toBeUndefined()
    expect(el.textContent).toContain('Ana')
    expect(el.textContent).toContain('No pude leer los aparatos del hogar')
    expect(el.textContent).not.toContain('No pude leer las personas')
  })

  it('con uno elegido no avisa', async () => {
    admin.getHome.mockResolvedValue(home('tv'))
    const { el, select } = await mount()
    expect(select.value).toBe('tv')
    expect(el.textContent).not.toContain('Elige dónde suenan los avisos')
  })
})
