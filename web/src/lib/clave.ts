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
