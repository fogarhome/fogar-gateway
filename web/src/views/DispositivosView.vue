<script setup lang="ts">
import { computed, onMounted, ref, shallowRef } from 'vue'
import { useRouter } from 'vue-router'
import { getMe, listMyDevices, retireMyDevice, updateMe } from '@/api/admin'
import type { MyDevice } from '@/api/types'
import AvisosEn from '@/components/AvisosEn.vue'
import FgBoton from '@/components/FgBoton.vue'
import FgEstado from '@/components/FgEstado.vue'
import { formatRelative } from '@/lib/format'
import { opcionAvisos } from '@/lib/notifyDevice'
import { toast } from '@/lib/toasts'
import { useSessionStore } from '@/stores/session'

// Dónde tienes la sesión abierta. Retirar uno corta ya sus conexiones y su acceso; sus
// conversaciones se quedan.
const router = useRouter()
const session = useSessionStore()
const items = shallowRef<MyDevice[]>([])
const loaded = ref(false)
const error = ref<string | null>(null)
const retirando = ref<string | null>(null)
// Mi dispositivo de avisos (#407): `undefined` hasta leer /auth/me.
const avisos = ref<string | null | undefined>(undefined)

async function cargar(): Promise<void> {
  try {
    const [lista, me] = await Promise.all([listMyDevices(), getMe()])
    items.value = lista
    avisos.value = me.notify_client_id
    error.value = null
  } catch {
    error.value = 'No pude leer tus dispositivos.'
  } finally {
    loaded.value = true
  }
}
onMounted(cargar)

const ahora = new Date()
const filas = computed(() =>
  items.value.map((d) => ({
    ...d,
    visto: d.last_seen_at ? formatRelative(d.last_seen_at, ahora) : 'Sin conectar todavía',
  })),
)

// Mis avisos solo pueden sonar en uno de mis aparatos activos (el gateway lo comprueba igual).
const opcionesAvisos = computed(() =>
  items.value.flatMap((d) => opcionAvisos(d, d.current ? `${d.name} (este)` : d.name) ?? []),
)
async function guardarAvisos(id: string | null): Promise<void> {
  avisos.value = (await updateMe({ notify_client_id: id })).notify_client_id
}

async function retirar(d: MyDevice): Promise<void> {
  const aviso = d.current
    ? 'Es este navegador: saldrás de la web.'
    : `«${d.name}» deja de tener acceso y se desconecta ya.`
  if (!window.confirm(`¿Retirar «${d.name}»? ${aviso}`)) return
  retirando.value = d.id
  try {
    await retireMyDevice(d.id)
    if (d.current) {
      session.signOut()
      await router.replace('/conectar')
      return
    }
    toast('ok', `«${d.name}» retirado`)
    // retirarlo lo quita de mis avisos si estaba elegido
    await cargar()
  } catch {
    toast('bad', 'No he podido retirarlo', 'Sigue con acceso. Prueba otra vez.')
  } finally {
    retirando.value = null
  }
}
</script>

<template>
  <div class="dispositivos">
    <header>
      <h1 class="fg-h1">Mis dispositivos</h1>
      <p class="fg-sub">
        Donde has entrado con tu cuenta. Lo que hablas desde ellos es tuyo: no aparece en la tele ni en los aparatos del
        hogar.
      </p>
    </header>
    <p v-if="error" class="fg-meta">{{ error }}</p>
    <div v-else-if="filas.length" class="fg-scroll">
      <table class="fg-tabla">
        <thead>
          <tr>
            <th>Dispositivo</th>
            <th class="opcional">Última vez</th>
            <th><span class="fg-solo-lector">Acciones</span></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="d in filas" :key="d.id">
            <td>
              {{ d.name }}
              <FgEstado v-if="d.current" tono="ok" texto="Este" />
            </td>
            <td class="fg-tenue opcional">{{ d.visto }}</td>
            <td class="accion">
              <FgBoton tam="sm" :ocupado="retirando === d.id" @click="retirar(d)">Retirar</FgBoton>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    <p v-else-if="loaded" class="fg-meta">No hay ninguno.</p>
    <AvisosEn
      v-if="!error && avisos !== undefined"
      etiqueta="Avisos en…"
      :opciones="opcionesAvisos"
      :elegido="avisos"
      :guardar="guardarAvisos"
    />
  </div>
</template>

<style scoped>
.dispositivos {
  display: flex;
  flex-direction: column;
  gap: 24px;
}
header {
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.accion {
  text-align: right;
}
</style>
