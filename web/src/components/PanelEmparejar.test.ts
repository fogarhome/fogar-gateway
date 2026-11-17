import { createPinia } from 'pinia'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createApp, h, nextTick } from 'vue'
import { ApiError } from '@/api/http'
import type { PairingOpened } from '@/api/types'
import PanelEmparejar from './PanelEmparejar.vue'

vi.mock('@/api/admin', () => ({
  openPairing: vi.fn(),
  getPairing: vi.fn(),
  cancelPairing: vi.fn(() => Promise.resolve()),
  listClients: vi.fn(() => Promise.resolve([])),
}))
vi.mock('@/lib/toasts', () => ({ toast: vi.fn() }))
const { toast } = vi.mocked(await import('@/lib/toasts'))
const admin = await import('@/api/admin')
const listClients = vi.mocked(admin.listClients)
const openPairing = vi.mocked(admin.openPairing)
const getPairing = vi.mocked(admin.getPairing)
const cancelPairing = vi.mocked(admin.cancelPairing)

const codigo = (id: string, expira = new Date(Date.now() + 600_000)): PairingOpened => ({
  id,
  code: '123456',
  state: 'open',
  expires_at: expira.toISOString(),
  client_id: null,
  client_name: null,
  template: {},
})

function pendiente<T>() {
  let resolve!: (v: T) => void
  const promise = new Promise<T>((r) => (resolve = r))
  return { promise, resolve }
}

function mount(onEmparejado = vi.fn()) {
  const el = document.createElement('div')
  const app = createApp({ render: () => h(PanelEmparejar, { onEmparejado }) }).use(createPinia())
  app.mount(el)
  const boton = (texto: string) =>
    [...el.querySelectorAll('button')].find((b) => b.textContent?.includes(texto)) as HTMLButtonElement
  return { el, app, boton }
}
const flush = async () => {
  for (let i = 0; i < 5; i++) await nextTick()
}

beforeEach(() => {
  vi.clearAllMocks()
  getPairing.mockResolvedValue({ ...codigo('c1'), state: 'open' })
})
afterEach(() => vi.useRealTimers())

describe('PanelEmparejar: ningún código queda abierto sin verse', () => {
  it('«Cancelar» con el POST en curso anula el código cuando llega', async () => {
    const p = pendiente<PairingOpened>()
    openPairing.mockReturnValue(p.promise)
    const { boton, app } = mount()
    boton('Cerrar').click()
    expect(cancelPairing).not.toHaveBeenCalled()
    p.resolve(codigo('c1'))
    await flush()
    expect(cancelPairing).toHaveBeenCalledWith('c1')
    app.unmount()
    expect(cancelPairing).toHaveBeenCalledTimes(1)
  })

  it('desmontar con el POST en curso anula el código cuando llega', async () => {
    const p = pendiente<PairingOpened>()
    openPairing.mockReturnValue(p.promise)
    const { app } = mount()
    app.unmount()
    p.resolve(codigo('c1'))
    await flush()
    expect(cancelPairing).toHaveBeenCalledWith('c1')
  })

  it('con el reloj del navegador adelantado, la cuenta atrás va desde que llega y «Cancelar» lo anula', async () => {
    vi.useFakeTimers({ toFake: ['Date', 'performance', 'setInterval', 'clearInterval'] })
    const servidor = new Date('2026-11-17T10:00:00Z')
    vi.setSystemTime(new Date(servidor.getTime() + 15 * 60_000)) // 15 min adelantado
    openPairing.mockResolvedValue(codigo('c1', new Date(servidor.getTime() + 600_000)))
    const { el, boton } = mount()
    await flush()
    vi.advanceTimersByTime(1000)
    await flush()
    expect(el.textContent).toContain('caduca en 9:54') // 10 min − 5 s de margen − 1 s
    boton('Cancelar').click()
    expect(cancelPairing).toHaveBeenCalledWith('c1')
  })

  it('caducado aquí: «Otro código» y el desmontaje anulan el anterior', async () => {
    openPairing.mockResolvedValueOnce(codigo('c1')).mockResolvedValueOnce(codigo('c2'))
    getPairing.mockResolvedValueOnce({ ...codigo('c1'), state: 'expired' })
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
    const { boton, app } = mount()
    await flush()
    vi.advanceTimersByTime(2000) // sondeo: el servidor dice expired
    await flush()
    boton('Otro código').click()
    await flush()
    expect(cancelPairing).toHaveBeenCalledWith('c1')
    app.unmount()
    expect(cancelPairing).toHaveBeenCalledWith('c2')
  })

  it('usado: no se anula', async () => {
    openPairing.mockResolvedValue(codigo('c1'))
    getPairing.mockResolvedValue({ ...codigo('c1'), state: 'used', client_id: 'x', client_name: 'Tele' })
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
    const { app } = mount()
    await flush()
    vi.advanceTimersByTime(2000)
    await flush()
    app.unmount()
    expect(cancelPairing).not.toHaveBeenCalled()
  })

  it('un sondeo del código anterior que llega tarde no pisa el estado del nuevo', async () => {
    const viejo = pendiente<PairingOpened>()
    openPairing.mockResolvedValueOnce(codigo('c1')).mockResolvedValueOnce(codigo('c2'))
    getPairing.mockReturnValue(viejo.promise) // los sondeos de c1 se quedan en vuelo
    vi.useFakeTimers({ toFake: ['performance', 'setInterval', 'clearInterval'] })
    const { el, boton } = mount()
    await flush()
    vi.advanceTimersByTime(596_000) // aquí caduca c1
    await flush()
    boton('Otro código').click()
    await flush()
    expect(el.textContent).toContain('caduca en')
    viejo.resolve({ ...codigo('c1'), state: 'expired' })
    await flush()
    expect(el.textContent).toContain('caduca en') // sigue enseñando c2
  })

  it('si el DELETE da 404 porque ya se usó, avisa del emparejamiento aunque el panel esté cerrado', async () => {
    openPairing.mockResolvedValue(codigo('c1'))
    cancelPairing.mockRejectedValueOnce(new ApiError('not-found', 404, 'Pairing not found or not open'))
    getPairing.mockResolvedValue({ ...codigo('c1'), state: 'used', client_id: 'x', client_name: 'Tele' })
    const onEmparejado = vi.fn()
    const { boton } = mount(onEmparejado)
    await flush()
    boton('Cancelar').click()
    await flush()
    expect(toast).toHaveBeenCalledWith('ok', 'Tele ya habla con Fogar', expect.any(String))
    expect(listClients).toHaveBeenCalled()
  })

  it('si el DELETE de «Otro código» da 404 porque ya se usó, emite emparejado', async () => {
    openPairing.mockResolvedValue(codigo('c1'))
    getPairing.mockResolvedValueOnce({ ...codigo('c1'), state: 'expired' })
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
    const onEmparejado = vi.fn()
    const { boton } = mount(onEmparejado)
    await flush()
    vi.advanceTimersByTime(2000)
    await flush()
    cancelPairing.mockRejectedValueOnce(new ApiError('not-found', 404, 'x'))
    getPairing.mockResolvedValue({ ...codigo('c1'), state: 'used', client_id: 'x', client_name: 'Tele' })
    boton('Otro código').click()
    await flush()
    expect(onEmparejado).toHaveBeenCalledWith('x', 'Tele')
  })
})
