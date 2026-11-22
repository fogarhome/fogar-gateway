<script setup lang="ts">
import { computed, ref, shallowRef, watch } from 'vue'
import { onBeforeRouteLeave, useRoute, useRouter } from 'vue-router'
import { deleteClient, getClient, rotateClientKey, updateClient } from '@/api/admin'
import { ApiError } from '@/api/http'
import type { ClientResponse, OutputMode } from '@/api/types'
import ClaveNueva from '@/components/ClaveNueva.vue'
import FgBoton from '@/components/FgBoton.vue'
import FgCampo from '@/components/FgCampo.vue'
import FgEstado from '@/components/FgEstado.vue'
import { frenarRecarga, releer, SALIR_CON_CLAVE, SALIR_EN_VUELO } from '@/lib/clave'
import { diff, toForm, validate, type ClientForm } from '@/lib/clientForm'
import { parseIso } from '@/lib/format'
import { CLIENT_TYPE, label } from '@/lib/labels'
import { toast } from '@/lib/toasts'
import { useClientsStore } from '@/stores/clients'
import { useLiveStore } from '@/stores/live'

const route = useRoute()
const router = useRouter()
const clients = useClientsStore()
const live = useLiveStore()

const id = computed(() => String(route.params.id))
const cliente = shallowRef<ClientResponse | null>(null)
const form = ref<ClientForm | null>(null)
const cargaError = ref<string | null>(null)
const huellaDudosa = ref(false) // G-r7-1: rotar sin respuesta y sin poder releer la huella
const comprobando = ref(false) // releyendo la huella: rotar otra vez ahora mezclaría las respuestas
let vuelta = 0 // cada comprobación, rotación o carga lo sube: una comprobación vieja ya no toca nada
async function cargar(): Promise<void> {
  try {
    cliente.value = await getClient(id.value)
    form.value = toForm(cliente.value)
    cargaError.value = null
    huellaDudosa.value = false
    comprobando.value = false
    vuelta++
  } catch (e) {
    cargaError.value = e instanceof ApiError && e.kind === 'not-found' ? 'Ese cliente ya no existe.' : 'No llego al gateway.'
  }
}
watch(id, () => void cargar(), { immediate: true })

const conectado = computed(() => (live.state?.sessions ?? []).some((s) => s.client_id === id.value))
const meta = computed(() => {
  const c = cliente.value
  if (!c) return ''
  const tipo = c.client_type ? label(CLIENT_TYPE, c.client_type) : 'Sin tipo'
  const fecha = parseIso(c.created_at).toLocaleDateString('es-ES', { day: 'numeric', month: 'short', year: 'numeric' })
  const huella = huellaDudosa.value ? 'sin comprobar' : `${c.key_fp.slice(0, 4)} ${c.key_fp.slice(4)}`
  // 4.0: de una persona (un inicio de sesión) o del hogar (emparejado), y lo que sabe hacer.
  const de = c.user_id ? 'de una persona' : 'del hogar'
  const sabe = c.capabilities?.length ? ` · sabe: ${c.capabilities.join(', ')}` : ''
  return `${tipo} · ${de} · creado el ${fecha} · huella ${huella}${sabe}`
})

const errores = computed(() => (form.value ? validate(form.value) : {}))
const cambios = computed(() => (cliente.value && form.value ? diff(cliente.value, form.value) : {}))
const hayCambios = computed(() => Object.keys(cambios.value).length > 0)
const intentado = ref(false)
const err = (k: keyof ClientForm) => (intentado.value ? (errores.value[k] ?? null) : null)
const guardando = ref(false)
const guardarError = ref<string | null>(null)

async function guardar(): Promise<void> {
  intentado.value = true
  if (Object.keys(errores.value).length || !hayCambios.value) return
  guardando.value = true
  guardarError.value = null
  try {
    cliente.value = await updateClient(id.value, cambios.value)
    form.value = toForm(cliente.value)
    intentado.value = false
    void clients.refresh()
    toast('ok', 'Cambios guardados', conectado.value ? 'La sesión abierta los toma al reconectarse.' : undefined)
  } catch (e) {
    guardarError.value =
      e instanceof ApiError && (e.kind === 'invalid' || e.kind === 'conflict')
        ? `El gateway no lo acepta: ${e.message}`
        : 'No he podido guardar. Prueba otra vez.'
  } finally {
    guardando.value = false
  }
}
function descartar() {
  if (cliente.value) form.value = toForm(cliente.value)
  intentado.value = false
  guardarError.value = null
}

const SALIDAS: { valor: OutputMode; texto: string }[] = [
  { valor: 'audio', texto: 'Voz' },
  { valor: 'text', texto: 'Texto' },
  { valor: 'status', texto: 'Avisos de estado' },
]

// Clave nueva: se ve una sola vez y, mientras está a la vista, lo demás queda inerte.
const claveNueva = ref<string | null>(null)
const confirmar = ref<'rotar' | 'borrar' | null>(null)
const nombreBorrar = ref('')
const trabajando = ref(false)
const rotando = ref(false) // aparte de trabajando: borrar sale de la página con trabajando a true

async function rotar(): Promise<void> {
  if (comprobando.value) return
  const pedido = id.value
  trabajando.value = true
  rotando.value = true
  try {
    const { client_key, ...resto } = await rotateClientKey(id.value)
    vuelta++
    huellaDudosa.value = false
    claveNueva.value = client_key
    cliente.value = resto // sin la clave: solo vive en claveNueva mientras se enseña
    void clients.refresh()
  } catch (e) {
    if (e instanceof ApiError && e.kind !== 'network') {
      toast('bad', 'No he podido rotar la clave', 'La clave de antes sigue valiendo.')
    } else {
      // G-r6-3: sin respuesta, el gateway pudo rotarla: la huella releída lo dice.
      toast('bad', 'No sé si se rotó la clave', 'Lo compruebo con la huella.')
      void comprobarHuella(pedido)
    }
  } finally {
    trabajando.value = false
    rotando.value = false
    confirmar.value = null
  }
}
/** G-r7-1: hasta releer la huella no se enseña ninguna como actual. Solo la huella (no cargar() ni
 * el cliente entero): no pisa el formulario ni los cambios sin guardar. */
async function comprobarHuella(pedido: string): Promise<void> {
  const antes = cliente.value?.key_fp
  const mia = ++vuelta
  huellaDudosa.value = true
  comprobando.value = true
  const c = await releer(() => getClient(pedido), (r) => r.key_fp !== antes)
  if (mia !== vuelta || id.value !== pedido || !cliente.value) return
  comprobando.value = false
  if (!c) {
    toast('bad', 'No he podido comprobarlo', 'Recarga la página para ver la huella: si ha cambiado, la clave de antes ya no vale.')
    return
  }
  huellaDudosa.value = false
  cliente.value = { ...cliente.value, key_fp: c.key_fp }
  if (c.key_fp !== antes) toast('bad', 'Sí se rotó la clave', 'La de antes ya no vale. Rótala otra vez para ver una nueva.')
  else toast('ok', 'La huella no ha cambiado', 'Parece que no se rotó. Si el aparato deja de entrar, recarga la página y mira la huella.')
}
async function cambiarActivo(): Promise<void> {
  const c = cliente.value
  if (!c) return
  trabajando.value = true
  try {
    cliente.value = await updateClient(id.value, { is_active: !c.is_active })
    void clients.refresh()
    toast('ok', cliente.value.is_active ? 'Activado' : 'Desactivado', cliente.value.is_active ? 'Ya puede volver a entrar.' : 'Sus sesiones se cierran en menos de 10 s.')
  } catch {
    toast('bad', 'No he podido cambiarlo', 'Prueba otra vez.')
  } finally {
    trabajando.value = false
  }
}
async function borrar(): Promise<void> {
  const c = cliente.value
  if (!c || nombreBorrar.value.trim() !== c.name) return
  trabajando.value = true
  try {
    await deleteClient(id.value)
  } catch (e) {
    // G-r7-4: 404 es que ya no existe (p. ej. un borrado anterior que se quedó sin respuesta)
    if (!(e instanceof ApiError && e.kind === 'not-found')) {
      toast('bad', 'No he podido borrarlo', 'Prueba otra vez.')
      trabajando.value = false
      return
    }
  }
  toast('ok', `«${c.name}» borrado`)
  void clients.refresh()
  form.value = toForm(c) // G-r6-4: ya borrado, los cambios sin guardar no frenan la salida
  await router.replace('/clientes')
}

onBeforeRouteLeave(() => {
  if (rotando.value) return window.confirm(SALIR_EN_VUELO)
  if (claveNueva.value) return window.confirm(SALIR_CON_CLAVE)
  if (hayCambios.value) return window.confirm('Hay cambios sin guardar. ¿Salir sin guardarlos?')
  return true
})
frenarRecarga(() => rotando.value)
</script>

<template>
  <div class="cliente">
    <RouterLink to="/clientes" class="fg-meta volver">← Clientes</RouterLink>
    <p v-if="cargaError" class="fg-sub">{{ cargaError }}</p>
    <template v-else-if="cliente && form">
      <header class="titulo">
        <h1 class="fg-h1">{{ cliente.name }}</h1>
        <p class="fg-meta meta">
          {{ meta }}
          <FgEstado v-if="!cliente.is_active" tono="off" texto="desactivado" />
          <FgEstado v-else-if="conectado" tono="ok" texto="conectado ahora" />
        </p>
      </header>

      <ClaveNueva
        v-if="claveNueva"
        :clave="claveNueva"
        :nombre="cliente.name"
        aviso="La clave de antes ya no vale: el aparato no vuelve a entrar hasta que use esta."
        @hecho="claveNueva = null"
      />

      <div class="partes" :inert="!!claveNueva" :class="{ inerte: claveNueva }">
        <form novalidate aria-labelledby="h-ajustes" class="ajustes" @submit.prevent="guardar">
          <h2 id="h-ajustes" class="fg-h2">Ajustes</h2>
          <div class="rejilla">
            <FgCampo v-model="form.name" etiqueta="Nombre" :error="err('name')" />
            <div class="campo-select">
              <label for="tipo">Tipo</label>
              <select id="tipo" v-model="form.client_type">
                <option value="">Sin tipo</option>
                <option v-for="(texto, valor) in CLIENT_TYPE" :key="valor" :value="valor">{{ texto }}</option>
              </select>
              <p class="fg-meta">Solo cambia cómo se ve aquí.</p>
            </div>
            <FgCampo v-model="form.default_agent" etiqueta="Agente por defecto" placeholder="El de OpenClaw" pista="Vacío: el agente por defecto de OpenClaw." :error="err('default_agent')" />
            <FgCampo v-model="form.tts_voice" etiqueta="Voz" pista="El nombre de la voz en el proveedor de voz." :error="err('tts_voice')" />
          </div>
          <fieldset class="salidas">
            <legend>Sabe reproducir</legend>
            <div class="opciones">
              <label v-for="s in SALIDAS" :key="s.valor" class="opcion">
                <input v-model="form.output_mode" type="checkbox" :value="s.valor" />{{ s.texto }}
              </label>
            </div>
            <p class="fg-meta">Lo dice el aparato al emparejarse. En cada sesión manda lo que pida al conectarse.</p>
          </fieldset>
          <details class="mas">
            <summary>Conversación y voz: idioma, interrupciones, silencio, mensajes de Fogar y herramientas</summary>
            <div class="rejilla">
              <FgCampo v-model="form.stt_language" etiqueta="Idioma al transcribir" pista="Código del idioma: es, en, gl…" :error="err('stt_language')" />
              <FgCampo v-model="form.stt_vad_thold" etiqueta="Umbral de voz" inputmode="decimal" pista="De 0 a 1. 0: el del transcriptor." :error="err('stt_vad_thold')" />
              <FgCampo v-model="form.tts_speed" etiqueta="Velocidad de la voz" inputmode="decimal" pista="1 es la normal." :error="err('tts_speed')" />
              <FgCampo v-model="form.allowed_agents" etiqueta="Agentes permitidos" pista="Separados por comas. Vacío: todos." :error="err('allowed_agents')" />
              <FgCampo v-model="form.barge_in_min_chars" etiqueta="Letras para interrumpir" inputmode="numeric" pista="Cuánto tiene que decir para cortar a Fogar." :error="err('barge_in_min_chars')" />
              <FgCampo v-model="form.silence_timeout_s" etiqueta="Silencio para cerrar el turno (s)" inputmode="decimal" :error="err('silence_timeout_s')" />
              <FgCampo v-model="form.max_silence_turns" etiqueta="Turnos en silencio antes de colgar" inputmode="numeric" :error="err('max_silence_turns')" />
            </div>
            <div class="interruptores">
              <label class="opcion"><input v-model="form.barge_in_enabled" type="checkbox" />Se le puede interrumpir</label>
              <label class="opcion"><input v-model="form.push_enabled" type="checkbox" />Fogar puede hablarle sin que pregunte</label>
              <label class="opcion"><input v-model="form.tool_calls_enabled" type="checkbox" />Ve las herramientas que usa Fogar</label>
            </div>
          </details>
          <div class="guardar">
            <template v-if="hayCambios">
              <FgBoton type="submit" variante="principal" :ocupado="guardando">Guardar cambios</FgBoton>
              <FgBoton @click="descartar">Descartar</FgBoton>
            </template>
            <span v-else class="fg-meta">Sin cambios</span>
            <p v-if="guardarError" class="fg-meta" role="alert">{{ guardarError }}</p>
          </div>
        </form>

        <section aria-labelledby="h-acceso" class="acceso">
          <h2 id="h-acceso" class="fg-h2">Acceso</h2>
          <div class="accion">
            <div><b>Rotar la clave</b><p class="fg-meta">Crea una clave nueva y desconecta el aparato hasta que la use.</p></div>
            <div v-if="confirmar === 'rotar'" class="confirmar">
              <FgBoton variante="principal" :ocupado="trabajando" @click="rotar">Rotar ahora</FgBoton>
              <FgBoton @click="confirmar = null">Cancelar</FgBoton>
            </div>
            <FgBoton v-else :disabled="comprobando" @click="confirmar = 'rotar'">Rotar clave…</FgBoton>
          </div>
          <div class="accion">
            <div>
              <b>{{ cliente.is_active ? 'Desactivar' : 'Activar' }}</b>
              <p class="fg-meta">{{ cliente.is_active ? 'Cierra sus sesiones en menos de 10 s y no lo deja entrar. Se puede volver a activar.' : 'Vuelve a dejarle entrar con su clave de siempre.' }}</p>
            </div>
            <FgBoton :ocupado="trabajando && confirmar === null" @click="cambiarActivo">{{ cliente.is_active ? 'Desactivar' : 'Activar' }}</FgBoton>
          </div>
          <div class="accion">
            <div>
              <b><FgEstado tono="bad" texto="Borrar" /></b>
              <p class="fg-meta">Para siempre: sus sesiones se cierran y la clave deja de existir.</p>
            </div>
            <form v-if="confirmar === 'borrar'" class="confirmar" @submit.prevent="borrar">
              <FgCampo v-model="nombreBorrar" :etiqueta="`Escribe «${cliente.name}» para borrarlo`" autocomplete="off" />
              <FgBoton type="submit" :disabled="nombreBorrar.trim() !== cliente.name" :ocupado="trabajando">Borrar para siempre</FgBoton>
              <FgBoton @click="confirmar = null">Cancelar</FgBoton>
            </form>
            <FgBoton v-else @click="(confirmar = 'borrar'), (nombreBorrar = '')">Borrar…</FgBoton>
          </div>
        </section>
      </div>
    </template>
    <p v-else class="fg-meta">Cargando…</p>
  </div>
</template>

<style scoped>
.cliente {
  display: flex;
  flex-direction: column;
  gap: 28px;
  max-width: 960px;
}
.volver {
  align-self: flex-start;
  color: var(--hueso-suave);
}
.volver:hover {
  color: var(--hueso);
}
.titulo {
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.meta {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 4px 12px;
}
.partes {
  display: flex;
  flex-direction: column;
  gap: 36px;
  transition: opacity var(--t-base) ease;
}
.inerte {
  opacity: 0.4;
}
.ajustes {
  display: flex;
  flex-direction: column;
  gap: 22px;
}
.rejilla {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 18px;
}
.campo-select {
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.campo-select label,
legend {
  font-size: 14px;
  font-weight: 550;
}
.campo-select select {
  height: 44px;
  padding: 0 12px;
  border-radius: var(--radio-nodo);
  border: 1px solid var(--veta);
  background: var(--noche);
  color: var(--hueso);
}
.salidas {
  margin: 0;
  padding: 0;
  border: 0;
  display: flex;
  flex-direction: column;
  gap: 10px;
}
legend {
  padding: 0 0 10px;
}
.opciones,
.interruptores {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}
.opcion {
  display: inline-flex;
  align-items: center;
  gap: 10px;
  min-height: 44px;
  padding: 0 16px;
  border-radius: var(--radio-nodo);
  background: var(--piedra);
  cursor: pointer;
}
.opcion:has(input:checked) {
  background: var(--piedra-alta);
}
.opcion input {
  width: 18px;
  height: 18px;
  margin: 0;
  accent-color: var(--hueso);
}
.mas {
  border-top: 1px solid var(--veta);
  padding-top: 16px;
}
.mas summary {
  cursor: pointer;
  font-weight: 550;
  min-height: 32px;
}
.mas[open] summary {
  margin-bottom: 18px;
}
.interruptores {
  margin-top: 18px;
}
.guardar {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 12px;
}
.guardar p {
  flex-basis: 100%;
}
.acceso {
  display: flex;
  flex-direction: column;
  gap: 4px;
  border-top: 1px solid var(--veta);
  padding-top: 22px;
}
.acceso h2 {
  margin-bottom: 8px;
}
.accion {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 12px 16px;
  padding: 12px 0;
}
.accion > div:first-child {
  flex: 1 1 320px;
}
.accion b {
  font-weight: 600;
}
.accion p {
  margin: 2px 0 0;
}
.confirmar {
  display: flex;
  flex-wrap: wrap;
  align-items: flex-end;
  gap: 10px;
}
@media (max-width: 760px) {
  .rejilla {
    grid-template-columns: 1fr;
  }
}
</style>
