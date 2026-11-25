<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { ApiError } from '@/api/http'
import { API_ERROR, ownText } from '@/lib/labels'
import { toast } from '@/lib/toasts'

// El dispositivo de avisos (5.0, #407): dónde suenan los recordatorios de Fogar. Un selector
// que guarda al cambiar y, mientras no haya uno elegido, el aviso «Elige dónde suenan…».
// Qué aparatos valen lo decide quien lo usa (el perfil, el hogar); el gateway lo comprueba igual.
export interface OpcionAvisos {
  id: string
  nombre: string
}

const props = defineProps<{
  etiqueta: string
  opciones: OpcionAvisos[]
  /** El elegido ahora; `null`, ninguno. */
  elegido: string | null
  guardar: (id: string | null) => Promise<unknown>
}>()

const valor = ref(props.elegido ?? '')
watch(
  () => props.elegido,
  (v) => (valor.value = v ?? ''),
)
const ocupado = ref(false)
// Uno elegido que ya no está en la lista (desactivado): se enseña tal cual, no se pierde sin querer.
const huerfano = computed(() => Boolean(props.elegido) && !props.opciones.some((o) => o.id === props.elegido))

async function cambiar(): Promise<void> {
  const antes = props.elegido ?? ''
  const nuevo = valor.value
  if (nuevo === antes) return
  ocupado.value = true
  try {
    await props.guardar(nuevo || null)
    toast('ok', nuevo ? 'Los avisos sonarán ahí' : 'Los avisos ya no suenan en ningún sitio')
  } catch (e) {
    valor.value = antes
    const texto = e instanceof ApiError ? ownText(API_ERROR, e.message) : undefined
    toast('bad', 'No he podido cambiarlo', texto ?? 'Prueba otra vez.')
  } finally {
    ocupado.value = false
  }
}
</script>

<template>
  <div class="avisos-en">
    <label>
      <span>{{ etiqueta }}</span>
      <select v-model="valor" :disabled="ocupado" @change="cambiar">
        <option value="">Ninguno</option>
        <option v-if="huerfano" :value="elegido">Uno que ya no está activo</option>
        <option v-for="o in opciones" :key="o.id" :value="o.id">{{ o.nombre }}</option>
      </select>
    </label>
    <p v-if="!elegido" class="aviso">
      <strong>Elige dónde suenan los avisos.</strong>
      Sin uno elegido, los recordatorios de Fogar no suenan en ningún sitio: solo se leen si alguien tiene abierta esa
      conversación.
    </p>
  </div>
</template>

<style scoped>
.avisos-en {
  display: flex;
  flex-direction: column;
  gap: 12px;
  max-width: 720px;
}
label {
  display: flex;
  flex-direction: column;
  gap: 8px;
  max-width: 360px;
}
/* Como el selector de rol de Personas. */
select {
  height: 44px;
  padding: 0 12px;
  border-radius: var(--radio-nodo);
  border: 1px solid var(--veta);
  background: var(--noche);
  color: var(--hueso);
}
.aviso {
  margin: 0;
  color: var(--hueso-suave);
}
.aviso strong {
  color: var(--hueso);
  font-weight: inherit;
}
</style>
