import { describe, expect, it } from 'vitest'
import { opcionAvisos, suena } from './notifyDevice'

describe('notifyDevice: qué aparatos pueden sonar un aviso', () => {
  it('quita los desactivados y los que tienen los avisos apagados', () => {
    expect(opcionAvisos({ id: 'a', name: 'A', is_active: false })).toBeNull()
    expect(opcionAvisos({ id: 'a', name: 'A', push_enabled: false })).toBeNull()
  })

  it('marca los que solo leen: sin audio en output_mode, o la web sin output_mode', () => {
    expect(suena({ id: 'a', name: 'A', output_mode: ['audio', 'text'] })).toBe(true)
    expect(suena({ id: 'a', name: 'A', output_mode: ['text'] })).toBe(false)
    expect(suena({ id: 'a', name: 'A', client_type: 'web', output_mode: null })).toBe(false)
    expect(suena({ id: 'a', name: 'A', client_type: 'app' })).toBe(true)
    expect(opcionAvisos({ id: 'a', name: 'Tele', output_mode: ['text'] })).toEqual({
      id: 'a',
      nombre: 'Tele (solo texto, no suena)',
    })
  })
})
