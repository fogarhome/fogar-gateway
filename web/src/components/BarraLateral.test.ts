import { createPinia } from 'pinia'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { createApp, h, nextTick } from 'vue'
import BarraLateral from './BarraLateral.vue'

vi.mock('vue-router', () => ({
  useRoute: () => ({ path: '/', meta: {} }),
  useRouter: () => ({ replace: vi.fn(), push: vi.fn() }),
  RouterLink: { props: ['to'], template: '<a><slot /></a>' },
}))

const montados: { unmount: () => void }[] = []

function montar(props: { nombre?: string; version?: string }) {
  const el = document.createElement('div')
  document.body.appendChild(el)
  const app = createApp({ render: () => h(BarraLateral, props) }).use(createPinia())
  app.mount(el)
  montados.push(app)
  return el
}

afterEach(() => {
  montados.splice(0).forEach((a) => a.unmount())
  document.body.innerHTML = ''
})

describe('BarraLateral: pie con la casa y la versión', () => {
  it('muestra «casa · vX» cuando hay nombre y versión', async () => {
    const el = montar({ nombre: 'Casa Garre', version: '5.0.0' })
    await nextTick()
    const pie = el.querySelector('.pie .fg-meta')?.textContent?.trim()
    expect(pie).toBe('Casa Garre · v5.0.0')
  })

  it('sin nombre muestra solo la versión', async () => {
    const el = montar({ version: '5.0.0' })
    await nextTick()
    expect(el.querySelector('.pie .fg-meta')?.textContent?.trim()).toBe('v5.0.0')
  })
})
