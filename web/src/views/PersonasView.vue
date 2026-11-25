<script setup lang="ts">
import { computed, onMounted, ref, shallowRef } from 'vue'
import { createUser, deleteUser, getHome, listClients, listUsers, updateHome, updateUser } from '@/api/admin'
import { ApiError } from '@/api/http'
import type { ClientResponse, Home, UserRole, UserRow } from '@/api/types'
import AvisosEn from '@/components/AvisosEn.vue'
import FgBoton from '@/components/FgBoton.vue'
import FgCampo from '@/components/FgCampo.vue'
import FgEstado from '@/components/FgEstado.vue'
import { toast } from '@/lib/toasts'
import { useSessionStore } from '@/stores/session'

// Las personas de casa (4.0). Solo dueños y administradores llegan aquí; un administrador no
// toca a un dueño (el gateway lo impide: 403 owner_only) y siempre queda un dueño (409 last_owner).
const session = useSessionStore()
const items = shallowRef<UserRow[]>([])
const home = shallowRef<Home | null>(null)
const aparatos = shallowRef<ClientResponse[]>([])
const loaded = ref(false)
const error = ref<string | null>(null)
const ocupado = ref<string | null>(null)

const ROLES: Record<UserRole, string> = { owner: 'Dueño', admin: 'Administrador', member: 'Miembro' }
const soyDueno = computed(() => session.user?.role === 'owner')

const MOTIVOS: Record<string, string> = {
  last_owner: 'Es el último dueño: el hogar siempre tiene uno. Haz dueño a otra persona antes.',
  owner_only: 'Eso solo lo puede hacer un dueño.',
  username_taken: 'Ese nombre de usuario ya existe.',
}
const motivo = (e: unknown) => (e instanceof ApiError && MOTIVOS[e.message]) || 'Prueba otra vez.'

async function cargar(): Promise<void> {
  try {
    ;[items.value, home.value, aparatos.value] = await Promise.all([listUsers(), getHome(), listClients()])
    error.value = null
  } catch {
    error.value = 'No pude leer las personas.'
  } finally {
    loaded.value = true
  }
}
onMounted(cargar)

// Los avisos de lo común (#407) pueden sonar en cualquier aparato activo del hogar, también en el
// de una persona activa; la lista ya viene sin los retirados.
const opcionesAvisos = computed(() => {
  const personas = new Map(items.value.map((u) => [u.id, u]))
  return aparatos.value.flatMap((c) => {
    if (!c.is_active) return []
    if (!c.user_id) return [{ id: c.id, nombre: c.name }]
    const u = personas.get(c.user_id)
    return u?.is_active ? [{ id: c.id, nombre: `${c.name} · de ${u.display_name}` }] : []
  })
})
async function guardarAvisos(id: string | null): Promise<void> {
  home.value = await updateHome({ notify_client_id: id })
}

const compartido = computed(() => items.value.some((u) => u.agent_shared))
// Un administrador no puede tocar a un dueño: sus controles no se enseñan.
const tocable = (u: UserRow) => soyDueno.value || u.role !== 'owner'

async function cambiar(u: UserRow, cambio: Parameters<typeof updateUser>[1], hecho: string): Promise<void> {
  ocupado.value = u.id
  try {
    await updateUser(u.id, cambio)
    toast('ok', hecho)
    await cargar()
  } catch (e) {
    toast('bad', 'No he podido cambiarlo', motivo(e))
  } finally {
    ocupado.value = null
  }
}

async function borrar(u: UserRow): Promise<void> {
  const texto =
    `¿Borrar a «${u.display_name}»? Se van sus dispositivos y sus conversaciones privadas del gateway ` +
    '(en OpenClaw quedan hasta que limpies su agente).'
  if (!window.confirm(texto)) return
  ocupado.value = u.id
  try {
    await deleteUser(u.id)
    toast('ok', `«${u.display_name}» borrado`)
    await cargar()
  } catch (e) {
    toast('bad', 'No he podido borrarlo', motivo(e))
  } finally {
    ocupado.value = null
  }
}

// Alta.
const nueva = ref(false)
const usuario = ref('')
const nombre = ref('')
const clave = ref('')
const rol = ref<UserRole>('member')
const altaError = ref<string | null>(null)
const creando = ref(false)

async function crear(): Promise<void> {
  if (creando.value) return
  if (!usuario.value.trim() || clave.value.length < 8) {
    altaError.value = 'Pon un usuario y una contraseña de 8 caracteres o más.'
    return
  }
  creando.value = true
  altaError.value = null
  try {
    const u = await createUser({
      username: usuario.value.trim(),
      display_name: nombre.value.trim() || usuario.value.trim(),
      password: clave.value,
      role: rol.value,
    })
    toast('ok', `«${u.display_name}» ya puede entrar`, `Con el usuario «${u.username}» y la contraseña que le has puesto.`)
    usuario.value = nombre.value = clave.value = ''
    rol.value = 'member'
    nueva.value = false
    await cargar()
  } catch (e) {
    altaError.value =
      e instanceof ApiError && e.kind === 'invalid'
        ? 'El usuario solo admite letras, cifras, punto, guion y guion bajo.'
        : motivo(e)
  } finally {
    creando.value = false
  }
}
</script>

<template>
  <div class="personas">
    <header class="titulo">
      <div class="texto">
        <h1 class="fg-h1">Personas</h1>
        <p class="fg-sub">
          Cada una entra con su cuenta. Lo que habla desde sus dispositivos es suyo; lo que se habla desde la tele o los
          altavoces es del hogar.
        </p>
      </div>
      <FgBoton variante="principal" icono="mas" :disabled="nueva" @click="nueva = true">Añadir una persona</FgBoton>
    </header>

    <form v-if="nueva" class="alta" novalidate @submit.prevent="crear">
      <FgCampo v-model="usuario" etiqueta="Usuario" autocomplete="off" />
      <FgCampo v-model="nombre" etiqueta="Nombre" pista="Como se verá en la web." />
      <FgCampo v-model="clave" etiqueta="Contraseña" type="password" autocomplete="new-password" pista="8 caracteres o más. Díselo en persona." :error="altaError" />
      <label class="rol">
        <span>Rol</span>
        <select v-model="rol">
          <option value="member">Miembro: lo suyo y lo común</option>
          <option value="admin">Administrador: el hogar entero</option>
          <option v-if="soyDueno" value="owner">Dueño</option>
        </select>
      </label>
      <div class="botones">
        <FgBoton type="submit" variante="principal" :ocupado="creando">Dar de alta</FgBoton>
        <FgBoton @click="nueva = false">Cancelar</FgBoton>
      </div>
    </form>

    <p v-if="error" class="fg-meta">{{ error }}</p>
    <div v-else-if="items.length" class="fg-scroll">
      <table class="fg-tabla">
        <thead>
          <tr>
            <th>Persona</th>
            <th>Rol</th>
            <th class="opcional">Agente</th>
            <th>Estado</th>
            <th><span class="fg-solo-lector">Acciones</span></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="u in items" :key="u.id">
            <td>
              {{ u.display_name }} <span class="fg-tenue">· {{ u.username }}</span>
            </td>
            <td>
              <select
                v-if="tocable(u) && u.id !== session.user?.id"
                :value="u.role"
                :aria-label="`Rol de ${u.display_name}`"
                :disabled="ocupado === u.id"
                @change="cambiar(u, { role: ($event.target as HTMLSelectElement).value as UserRole }, 'Rol cambiado')"
              >
                <option value="member">{{ ROLES.member }}</option>
                <option value="admin">{{ ROLES.admin }}</option>
                <option v-if="soyDueno" value="owner">{{ ROLES.owner }}</option>
              </select>
              <span v-else>{{ ROLES[u.role] }}</span>
            </td>
            <td class="opcional">
              {{ u.effective_agent ?? '?' }}<span v-if="u.agent_shared" class="fg-tenue"> · compartido</span>
            </td>
            <td>
              <FgEstado v-if="u.is_active" tono="ok" texto="Activa" />
              <span v-else class="fg-tenue">Desactivada</span>
            </td>
            <td class="accion">
              <template v-if="tocable(u) && u.id !== session.user?.id">
                <FgBoton
                  tam="sm"
                  :ocupado="ocupado === u.id"
                  @click="cambiar(u, { is_active: !u.is_active }, u.is_active ? 'Desactivada: sus dispositivos se han desconectado' : 'Activada')"
                >
                  {{ u.is_active ? 'Desactivar' : 'Activar' }}
                </FgBoton>
                <FgBoton tam="sm" :disabled="ocupado === u.id" @click="borrar(u)">Borrar</FgBoton>
              </template>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    <p v-else-if="loaded" class="fg-meta">Todavía no hay nadie.</p>

    <p v-if="compartido" class="aviso">
      Las personas con el agente «compartido» hablan con el mismo agente de OpenClaw: su memoria es común aunque las
      conversaciones sean privadas, y puede contar en un sitio algo dicho en otro. Para separarlos, dale a cada una su
      agente (<code>fogar-gw users edit USUARIO --agent NOMBRE</code>).
    </p>
    <p v-if="home" class="fg-meta">Lo común lo atiende «{{ home.agent ?? 'el agente por defecto' }}».</p>
    <AvisosEn
      v-if="home"
      etiqueta="Avisos del hogar en…"
      :opciones="opcionesAvisos"
      :elegido="home.notify_client_id ?? null"
      :guardar="guardarAvisos"
    />
  </div>
</template>

<style scoped>
.personas {
  display: flex;
  flex-direction: column;
  gap: 24px;
}
.titulo {
  display: flex;
  flex-wrap: wrap;
  align-items: flex-end;
  justify-content: space-between;
  gap: 16px;
}
.texto {
  display: flex;
  flex-direction: column;
  gap: 8px;
  max-width: 640px;
}
.alta {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 16px;
  align-items: start;
  max-width: 960px;
}
.rol {
  display: flex;
  flex-direction: column;
  gap: 8px;
}
/* Como el selector de agente de Conversaciones. */
select {
  height: 44px;
  padding: 0 12px;
  border-radius: var(--radio-nodo);
  border: 1px solid var(--veta);
  background: var(--noche);
  color: var(--hueso);
}
td select {
  height: 36px;
}
.botones {
  display: flex;
  gap: 12px;
  align-items: center;
  grid-column: 1 / -1;
}
.accion {
  display: flex;
  gap: 8px;
  justify-content: flex-end;
}
.aviso {
  margin: 0;
  max-width: 720px;
  color: var(--hueso-suave);
}
.aviso code {
  color: var(--hueso);
}
</style>
