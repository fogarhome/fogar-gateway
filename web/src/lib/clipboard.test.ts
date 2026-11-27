import { afterEach, describe, expect, it, vi } from 'vitest'
import { copyText } from './clipboard'

// Por http://IP (p. ej. la IP de Tailscale) no hay `navigator.clipboard`: queda la vía antigua.

describe('copyText sin portapapeles moderno', () => {
  afterEach(() => {
    Reflect.deleteProperty(document, 'execCommand')
    document.body.innerHTML = ''
  })

  it('sin campo, copia desde uno temporal, lo quita y devuelve el foco', async () => {
    const boton = document.body.appendChild(document.createElement('button'))
    boton.focus()
    let copiado = ''
    Object.defineProperty(document, 'execCommand', {
      configurable: true,
      value: vi.fn(() => {
        const el = document.querySelector('textarea')!
        copiado = el.value.slice(el.selectionStart, el.selectionEnd)
        return true
      }),
    })
    expect(await copyText('123456')).toBe(true)
    expect(copiado).toBe('123456')
    expect(document.querySelector('textarea')).toBeNull()
    expect(document.activeElement).toBe(boton)
  })

  it('si el navegador no copia, lo dice', async () => {
    Object.defineProperty(document, 'execCommand', { configurable: true, value: vi.fn(() => false) })
    expect(await copyText('123456')).toBe(false)
    expect(document.querySelector('textarea')).toBeNull()
  })
})
