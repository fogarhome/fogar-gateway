import { afterEach, describe, expect, it, vi } from 'vitest'
import { createApp, h, nextTick } from 'vue'
import { ApiError } from '@/api/http'
import AvisosEn from './AvisosEn.vue'

vi.mock('@/lib/toasts', () => ({ toast: vi.fn() }))
const { toast } = vi.mocked(await import('@/lib/toasts'))

const OPCIONES = [
  { id: 'd1', nombre: 'Móvil' },
  { id: 'd2', nombre: 'Tablet' },
]

function mount(elegido: string | null, guardar = vi.fn(() => Promise.resolve())) {
  const el = document.createElement('div')
  createApp({ render: () => h(AvisosEn, { etiqueta: 'Avisos en…', opciones: OPCIONES, elegido, guardar }) }).mount(el)
  const select = el.querySelector('select') as HTMLSelectElement
  return { el, select, guardar }
}
const flush = async () => {
  for (let i = 0; i < 5; i++) await nextTick()
}
const elegir = async (select: HTMLSelectElement, value: string) => {
  select.value = value
  select.dispatchEvent(new Event('change'))
  await flush()
}

afterEach(() => vi.clearAllMocks())

describe('AvisosEn: dónde suenan los avisos', () => {
  it('lista «Ninguno» y las opciones, con el elegido marcado', () => {
    const { el, select } = mount('d2')
    expect([...select.options].map((o) => o.textContent)).toEqual(['Ninguno', 'Móvil', 'Tablet'])
    expect(select.value).toBe('d2')
    expect(el.textContent).toContain('Avisos en…')
  })

  it('sin ninguno elegido, avisa «Elige dónde suenan los avisos»', () => {
    expect(mount(null).el.textContent).toContain('Elige dónde suenan los avisos')
    expect(mount('d1').el.textContent).not.toContain('Elige dónde suenan los avisos')
  })

  it('guarda al cambiar; «Ninguno» guarda null', async () => {
    const { select, guardar } = mount('d1')
    await elegir(select, 'd2')
    expect(guardar).toHaveBeenLastCalledWith('d2')
    await elegir(select, '')
    expect(guardar).toHaveBeenLastCalledWith(null)
    expect(toast).toHaveBeenCalledWith('ok', expect.any(String))
  })

  it('un 422 invalid_notify_device vuelve al de antes y lo explica', async () => {
    const guardar = vi.fn(() => Promise.reject(new ApiError('invalid', 422, 'invalid_notify_device')))
    const { select } = mount('d1', guardar)
    await elegir(select, 'd2')
    expect(select.value).toBe('d1')
    expect(toast).toHaveBeenCalledWith('bad', 'No he podido cambiarlo', expect.stringContaining('no puede recibir los avisos'))
  })

  it('un elegido que ya no está en la lista se enseña, no se pierde, y avisa', () => {
    const { el, select } = mount('d9')
    expect(select.value).toBe('d9')
    expect(select.options[1]?.textContent).toBe('Uno que ya no puede recibirlos')
    expect(el.textContent).toContain('Elige dónde suenan los avisos')
    expect(el.textContent).toContain('ya no puede recibirlos')
  })

  it('el aviso va en una región viva', () => {
    const { el } = mount(null)
    expect(el.querySelector('[role="status"]')?.textContent).toContain('Elige dónde suenan los avisos')
  })
})
