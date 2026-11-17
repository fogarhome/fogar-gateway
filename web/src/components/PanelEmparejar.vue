<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, shallowRef } from 'vue'
import { cancelPairing, getPairing, listPairings, openPairing } from '@/api/admin'
import { ApiError } from '@/api/http'
import type { PairingOpened, PairingState } from '@/api/types'
import { toast } from '@/lib/toasts'
import { useClientsStore } from '@/stores/clients'
import FgBoton from './FgBoton.vue'

// Emparejar un aparato: abre un código de 6 cifras (10 min, un solo uso) y espera a que el aparato lo
// canjee. El código solo vive aquí: no se guarda ni se registra. Al cerrar el panel, se anula.
// La cuenta atrás va desde que llega el código, no contra `expires_at`: el reloj del navegador puede
// ir desfasado. 600 s es CODE_TTL_S de services/pairing.py (el servidor no manda la duración); el
// margen cubre la ida y vuelta del POST, para no enseñar como válido un código que el servidor ya caducó.
const DURACION_MS = 600_000
const MARGEN_MS = 5_000
const emit = defineEmits<{ cerrar: []; emparejado: [clientId: string, nombre: string | null] }>()

const abierto = shallowRef<PairingOpened | null>(null)
const estado = ref<PairingState | 'abriendo' | 'error'>('abriendo')
const error = ref('')
const lleno = ref(false) // 409: ya hay 5 códigos abiertos
const clients = useClientsStore()
const ahora = ref(performance.now())
let recibido = 0
let fuera = false // el panel ya se cerró o se desmontó
let sondeo: ReturnType<typeof setInterval> | undefined
let reloj: ReturnType<typeof setInterval> | undefined

/** Anula el código recibido salvo que ya se haya usado: aunque aquí parezca caducado, puede seguir abierto. */
function anular(): void {
  const p = abierto.value
  abierto.value = null
  if (p && estado.value !== 'used') void cancelPairing(p.id).catch((e) => void siYaSeUso(p, e))
}

/** Recargar o cerrar la pestaña no desmonta el panel: el código se anula con un DELETE que
 * sobrevive a la página (G-r6-1). Si vuelve de la caché del navegador, ofrece otro código. */
function alSalirDeLaPagina(): void {
  const p = abierto.value
  if (!p || estado.value === 'used') return
  abierto.value = null
  estado.value = 'cancelled'
  void cancelPairing(p.id, true).catch(() => undefined)
}

/** Con 5 códigos abiertos (p. ej. de pestañas cerradas a destiempo), los anula todos y abre uno. */
async function anularAbiertos(): Promise<void> {
  estado.value = 'abriendo'
  try {
    const abiertos = await listPairings()
    await Promise.all(abiertos.map((p) => cancelPairing(p.id).catch(() => undefined)))
  } catch {
    // abrir() dirá si sigue sin poder
  }
  await abrir()
}

/** El DELETE da 404 si el código ya no estaba abierto: puede que un aparato acabe de canjearlo. */
async function siYaSeUso(p: PairingOpened, e: unknown): Promise<void> {
  if (!(e instanceof ApiError && e.kind === 'not-found')) return
  const r = await getPairing(p.id).catch(() => null)
  if (r?.state !== 'used' || !r.client_id) return
  if (!fuera) return emit('emparejado', r.client_id, r.client_name)
  // Panel ya cerrado: no queda quien escuche el evento.
  toast('ok', `${r.client_name ?? 'El aparato'} ya habla con Fogar`, 'Revisa sus ajustes si quieres cambiarlos.')
  void clients.refresh()
}

async function abrir(): Promise<void> {
  anular() // «Otro código» no deja el anterior abierto
  estado.value = 'abriendo'
  try {
    const p = await openPairing()
    if (fuera) {
      void cancelPairing(p.id).catch(() => undefined)
      return
    }
    abierto.value = p
    recibido = performance.now()
    ahora.value = recibido
    estado.value = 'open'
  } catch (e) {
    if (fuera) return
    estado.value = 'error'
    lleno.value = e instanceof ApiError && e.kind === 'conflict'
    error.value = lleno.value
      ? 'Ya hay 5 códigos abiertos. Espera a que caduquen o anúlalos.'
      : 'No he podido abrir un código. Prueba otra vez.'
  }
}

async function comprobar(): Promise<void> {
  const p = abierto.value
  if (!p || estado.value !== 'open') return
  try {
    const r = await getPairing(p.id)
    if (abierto.value !== p) return // respuesta de un código anterior
    estado.value = r.state
    if (r.state === 'used' && r.client_id) emit('emparejado', r.client_id, r.client_name)
  } catch {
    // se reintenta en el siguiente sondeo
  }
}

const restante = computed(() => {
  const p = abierto.value
  if (!p) return 0
  return Math.max(0, Math.round((recibido + DURACION_MS - MARGEN_MS - ahora.value) / 1000))
})
const cuenta = computed(() => `${Math.floor(restante.value / 60)}:${String(restante.value % 60).padStart(2, '0')}`)
const progreso = computed(() => `${Math.min(100, ((restante.value * 1000) / (DURACION_MS - MARGEN_MS)) * 100)}%`)
const codigo = computed(() => abierto.value?.code ?? '')
const codigoVisible = computed(() => `${codigo.value.slice(0, 3)} ${codigo.value.slice(3)}`)

async function cerrar(): Promise<void> {
  fuera = true
  anular()
  emit('cerrar')
}

onMounted(() => {
  window.addEventListener('pagehide', alSalirDeLaPagina)
  void abrir()
  sondeo = setInterval(() => void comprobar(), 2000)
  reloj = setInterval(() => {
    ahora.value = performance.now()
    if (estado.value === 'open' && restante.value === 0) estado.value = 'expired'
  }, 1000)
})
onBeforeUnmount(() => {
  window.removeEventListener('pagehide', alSalirDeLaPagina)
  clearInterval(sondeo)
  clearInterval(reloj)
  fuera = true
  anular()
})
</script>

<template>
  <section class="emparejar" aria-labelledby="h-emp">
    <div class="cab">
      <h2 id="h-emp" class="fg-h2">Emparejar un aparato</h2>
      <FgBoton tam="sm" @click="cerrar">{{ estado === 'open' ? 'Cancelar' : 'Cerrar' }}</FgBoton>
    </div>
    <template v-if="estado === 'open' || estado === 'abriendo'">
      <p class="fg-suave">Escribe este código en el aparato nuevo, o díselo en voz alta si te lo pide.</p>
      <p class="codigo" :aria-label="`Código ${codigo.split('').join(' ')}`">{{ estado === 'abriendo' ? '··· ···' : codigoVisible }}</p>
      <div class="tiempo">
        <div class="pista" aria-hidden="true"><div class="barra" :style="{ width: progreso }" /></div>
        <p class="fg-meta pie"><span>Esperando al aparato…</span><span>caduca en {{ cuenta }}</span></p>
      </div>
      <p class="fg-meta nota">El aparato pondrá su nombre y su tipo; después puedes cambiarlos. Un código vale una sola vez.</p>
    </template>
    <template v-else-if="estado === 'expired' || estado === 'cancelled'">
      <p class="fg-suave">El código ha caducado sin que ningún aparato lo usara.</p>
      <FgBoton variante="principal" @click="abrir">Otro código</FgBoton>
    </template>
    <template v-else-if="estado === 'error'">
      <p class="fg-suave">{{ error }}</p>
      <FgBoton v-if="lleno" variante="principal" @click="anularAbiertos">Anular los abiertos</FgBoton>
      <FgBoton @click="abrir">Probar otra vez</FgBoton>
    </template>
    <p v-else class="fg-suave">Emparejado.</p>
  </section>
</template>

<style scoped>
.emparejar {
  position: relative;
  display: flex;
  flex-direction: column;
  gap: 16px;
  padding: 24px;
  border-radius: var(--radio-vidrio);
  background: rgb(11 16 18 / 0.36);
  -webkit-backdrop-filter: blur(20px) saturate(1.25);
  backdrop-filter: blur(20px) saturate(1.25);
  box-shadow: var(--sombra-vidrio);
}
.emparejar p {
  margin: 0;
}
.cab {
  display: flex;
  align-items: center;
  gap: 12px;
}
.cab h2 {
  flex: 1;
}
.codigo {
  font: 500 56px/1 var(--font-texto);
  font-variant-numeric: tabular-nums;
  letter-spacing: 0.06em;
}
.tiempo {
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.pista {
  height: 4px;
  border-radius: 4px;
  background: var(--veta);
}
.barra {
  height: 4px;
  border-radius: 4px;
  background: var(--hueso);
  transition: width 1s linear;
}
.pie {
  display: flex;
  justify-content: space-between;
}
.nota {
  padding-top: 14px;
  border-top: 1px solid var(--veta);
}
</style>
