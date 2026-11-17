import { onBeforeUnmount, onMounted } from 'vue'

export const SALIR_CON_CLAVE = 'La clave nueva no se vuelve a enseñar. ¿Salir sin copiarla?'
export const SALIR_EN_VUELO = 'La clave nueva está llegando y solo se enseña aquí. ¿Salir sin verla?'

/** Mientras se crea o se rota una clave, recargar o cerrar la pestaña pide confirmación (ClaveNueva cubre después). */
export function frenarRecarga(enVuelo: () => boolean): void {
  const antesDeSalir = (e: BeforeUnloadEvent) => {
    if (enVuelo()) e.preventDefault()
  }
  onMounted(() => window.addEventListener('beforeunload', antesDeSalir))
  onBeforeUnmount(() => window.removeEventListener('beforeunload', antesDeSalir))
}

/**
 * Tras un fallo de red al crear o rotar (G-r7-1): relee hasta 3 veces, 1 s antes de cada lectura
 * (una lectura inmediata puede adelantarse a la escritura). Para cuando `basta` lo da por bueno.
 * Devuelve la lectura del último intento, o null si ese no contestó: una lectura anterior pudo adelantarse.
 */
export async function releer<T>(leer: () => Promise<T>, basta: (r: T) => boolean = () => true): Promise<T | null> {
  let ultima: T | null = null
  for (let i = 0; i < 3; i++) {
    await new Promise((r) => setTimeout(r, 1000))
    try {
      ultima = await leer()
      if (basta(ultima)) break
    } catch {
      ultima = null // sin respuesta: otro intento
    }
  }
  return ultima
}
