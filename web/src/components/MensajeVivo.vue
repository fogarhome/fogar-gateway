<script setup lang="ts">
import { computed } from 'vue'
import { endReasonText, progressText, type ChatLine } from '@/lib/chat'
import { formatClock } from '@/lib/format'
import FgBoton from './FgBoton.vue'
import MensajeConversacion from './MensajeConversacion.vue'

// Una línea del chat en vivo. Cada una calcula lo suyo: un token nuevo solo vuelve a pintar la
// respuesta que lo recibe, no toda la conversación.
// Un mensaje pendiente (5.0) se puede quitar, lo enviara quien lo enviara; el primero de la cola
// también puede pasar ya («Enviar ahora»: para lo que se está respondiendo).
const props = defineProps<{ linea: ChatLine; ahora: Date; primeroEnCola?: boolean }>()
defineEmits<{ quitar: [id: string]; ahora: [] }>()

const aviso = computed(() => {
  const l = props.linea
  if (l.unsent) return 'No se envió: no se pudo reabrir la conversación.'
  if (l.messageState === 'pending') return 'En cola: espera a que termine la respuesta en curso.'
  if (l.messageState === 'withdrawn') return 'Quitado: no llegó a Fogar.'
  if (l.state !== 'ended') return null
  return [l.otherConversation ? 'Es de otra conversación del panel.' : null, endReasonText(l)].filter(Boolean).join(' ') || null
})
// Mientras Fogar no escribe: en qué está («Pensando…», «Usando …»).
const progreso = computed(() => {
  const l = props.linea
  return l.state === 'streaming' && l.progress ? progressText(l.progress) : null
})
const hora = computed(() => (props.linea.at ? formatClock(props.linea.at, props.ahora) : null))
</script>

<template>
  <MensajeConversacion
    :quien="linea.role === 'user' ? (linea.from && !linea.from.self ? linea.from.name || 'Otro aparato' : 'Tú') : 'Fogar'"
    :propio="linea.role === 'user'"
    :texto="linea.text"
    :hora="hora"
    :push="linea.origin === 'push'"
    :escribiendo="linea.state === 'streaming'"
    :progreso="progreso"
    :aviso="aviso"
  />
  <div v-if="linea.messageState === 'pending' && linea.messageId" class="acciones">
    <FgBoton v-if="primeroEnCola" tam="sm" @click="$emit('ahora')">Enviar ahora</FgBoton>
    <FgBoton tam="sm" @click="$emit('quitar', linea.messageId)">Quitar</FgBoton>
  </div>
</template>

<style scoped>
.acciones {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  margin-top: -12px;
}
</style>
