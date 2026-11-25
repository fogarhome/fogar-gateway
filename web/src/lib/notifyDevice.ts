import type { OutputMode } from '@/api/types'

// El dispositivo de avisos (5.0, #407): qué aparatos se ofrecen y cuáles suenan. El gateway
// ignora al repartir un aparato desactivado o con los avisos apagados, y solo le da audio si su
// sesión lo pide: la web no ofrece los primeros y marca los que solo leen.

export interface OpcionAvisos {
  id: string
  nombre: string
}

export interface AparatoAvisos {
  id: string
  name: string
  client_type?: string | null
  is_active?: boolean
  push_enabled?: boolean
  output_mode?: OutputMode[] | null
}

/** Puede sonar: su `output_mode` lleva audio; sin él, todo menos la web (que solo lee). */
export function suena(a: AparatoAvisos): boolean {
  if (a.output_mode?.length) return a.output_mode.includes('audio')
  return a.client_type !== 'web'
}

/** La opción del selector, o `null` si no puede recibir avisos (desactivado o con los avisos apagados). */
export function opcionAvisos(a: AparatoAvisos, nombre: string = a.name): OpcionAvisos | null {
  if (a.is_active === false || a.push_enabled === false) return null
  return { id: a.id, nombre: suena(a) ? nombre : `${nombre} (solo texto, no suena)` }
}
