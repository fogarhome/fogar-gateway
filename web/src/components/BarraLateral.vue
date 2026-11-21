<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { api } from '@/api/http'
import { useClientsStore } from '@/stores/clients'
import { useLiveStore } from '@/stores/live'
import { useSessionStore } from '@/stores/session'
import FgIcono, { type IconoNombre } from './FgIcono.vue'
import FgPalabra from './FgPalabra.vue'

// Navegación: barra lateral en escritorio; en el móvil, cabecera con menú y barra inferior.
// Sin barra superior y sin anunciar lo que aún no existe. El chat vive en Conversaciones (M19).
const props = defineProps<{ nombre?: string; version?: string }>()

const route = useRoute()
const router = useRouter()
const live = useLiveStore()
const clients = useClientsStore()
const session = useSessionStore()
const menu = ref(false)

const abiertas = computed(() => live.state?.sessions.length ?? null)
const secciones = computed<{ to: string; texto: string; icono: IconoNombre; cuenta: string | null; activa: boolean }[]>(() => {
  const todas = [
    { to: '/', texto: 'Estado', icono: 'estado' as const, cuenta: null, activa: route.path === '/', admin: true },
    {
      to: '/conversaciones',
      texto: 'Conversaciones',
      icono: 'conversaciones' as const,
      cuenta: null,
      activa: route.path.startsWith('/conversaciones'),
      admin: false,
    },
    {
      to: '/sesiones',
      texto: 'Sesiones',
      icono: 'sesiones' as const,
      cuenta: abiertas.value ? `${abiertas.value} ${abiertas.value === 1 ? 'abierta' : 'abiertas'}` : null,
      activa: route.path.startsWith('/sesiones'),
      admin: true,
    },
    {
      to: '/clientes',
      texto: 'Clientes',
      icono: 'clientes' as const,
      cuenta: clients.loaded ? String(clients.items.length) : null,
      activa: route.path.startsWith('/clientes'),
      admin: true,
    },
    { to: '/personas', texto: 'Personas', icono: 'personas' as const, cuenta: null, activa: route.path.startsWith('/personas'), admin: true },
    {
      to: '/dispositivos',
      texto: 'Mis dispositivos',
      icono: 'dispositivos' as const,
      cuenta: null,
      activa: route.path.startsWith('/dispositivos'),
      admin: false,
    },
  ]
  // Un miembro ve lo suyo y lo común: sin el estado, las sesiones, los clientes ni las personas.
  return todas.filter((s) => session.isAdmin || !s.admin)
})

// En el móvil caben cuatro abajo: a quien cuida del hogar, las de siempre; Personas y Mis
// dispositivos van al menú de la cabecera.
const EN_MENU = ['/personas', '/dispositivos']
const inferior = computed(() => (session.isAdmin ? secciones.value.filter((s) => !EN_MENU.includes(s.to)) : secciones.value))
const enMenu = computed(() => secciones.value.filter((s) => !inferior.value.includes(s)))

watch(() => route.fullPath, () => (menu.value = false))

// Salir retira este navegador como dispositivo (su token deja de valer), no solo lo olvida.
async function salir(): Promise<void> {
  try {
    await api('/auth/logout', { method: 'POST' })
  } catch {
    // sin red o ya retirado: se olvida igual
  }
  session.signOut()
  void router.replace('/conectar')
}
</script>

<template>
  <nav class="barra" aria-label="Secciones">
    <RouterLink :to="session.isAdmin ? '/' : '/conversaciones'" class="marca" aria-label="Fogar Gateway, inicio">
      <FgPalabra class="palabra" />
      <span class="fg-meta">Gateway</span>
    </RouterLink>
    <div class="enlaces">
      <RouterLink v-for="s in secciones" :key="s.to" :to="s.to" :aria-current="s.activa ? 'page' : undefined">
        <FgIcono :nombre="s.icono" />{{ s.texto }}<small v-if="s.cuenta">{{ s.cuenta }}</small>
      </RouterLink>
    </div>
    <div class="pie">
      <span v-if="session.user" class="fg-meta">{{ session.user.display_name }}</span>
      <span v-if="props.nombre" class="fg-meta">{{ props.nombre }}<template v-if="props.version"> · v{{ props.version }}</template></span>
      <button type="button" class="salir" @click="salir">Salir</button>
    </div>
  </nav>

  <header class="cabecera">
    <RouterLink :to="session.isAdmin ? '/' : '/conversaciones'" aria-label="Fogar Gateway, inicio"><FgPalabra class="palabra" /></RouterLink>
    <button type="button" class="menu-boton" :aria-expanded="menu" aria-controls="menu-movil" aria-label="Menú" @click="menu = !menu">
      <FgIcono :nombre="menu ? 'cerrar' : 'menu'" />
    </button>
  </header>
  <div v-if="menu" id="menu-movil" class="menu-movil">
    <RouterLink v-for="s in enMenu" :key="s.to" :to="s.to" class="menu-enlace" :aria-current="s.activa ? 'page' : undefined">
      <FgIcono :nombre="s.icono" />{{ s.texto }}
    </RouterLink>
    <span v-if="props.nombre" class="fg-meta">{{ props.nombre }}<template v-if="props.version"> · v{{ props.version }}</template></span>
    <button type="button" class="salir" @click="salir">Salir</button>
  </div>
  <nav class="inferior" aria-label="Secciones">
    <RouterLink v-for="s in inferior" :key="s.to" :to="s.to" :aria-current="s.activa ? 'page' : undefined">
      <FgIcono :nombre="s.icono" :tam="20" />{{ s.texto }}
    </RouterLink>
  </nav>
</template>

<style scoped>
.barra {
  position: sticky;
  top: 0;
  align-self: flex-start;
  flex: 0 0 232px;
  height: 100vh;
  padding: 24px 14px;
  display: flex;
  flex-direction: column;
  gap: 28px;
  border-right: 1px solid var(--veta);
}
.marca {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 0 10px;
}
.marca .fg-meta {
  padding-top: 6px;
}
.palabra {
  height: 28px;
  width: auto;
  color: var(--hueso);
}
.enlaces {
  display: flex;
  flex-direction: column;
  gap: 2px;
}
.enlaces a {
  display: flex;
  align-items: center;
  gap: 12px;
  height: 40px;
  padding: 0 10px;
  border-radius: 10px;
  color: var(--hueso-suave);
  font-weight: 550;
  transition:
    background-color var(--t-rapido) ease,
    color var(--t-rapido) ease;
}
.enlaces a:hover {
  color: var(--hueso);
  background: rgb(236 230 220 / 0.04);
}
.enlaces a[aria-current='page'] {
  color: var(--hueso);
  background: var(--piedra-alta);
}
.enlaces small {
  margin-left: auto;
  font-weight: 400;
  font-size: 13px;
  color: var(--hueso-tenue);
}
.pie {
  margin-top: auto;
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 10px;
  padding: 0 10px;
}
.salir {
  height: 36px;
  padding: 0 14px;
  border: 0;
  border-radius: 999px;
  background: var(--velo);
  color: var(--hueso);
  font-weight: 550;
  font-size: 14px;
  cursor: pointer;
}
.salir:hover {
  background: var(--velo-alto);
}
.cabecera,
.inferior,
.menu-movil {
  display: none;
}
.menu-enlace {
  display: flex;
  align-items: center;
  gap: 10px;
  flex: 1 1 100%;
  min-height: 44px;
  color: var(--hueso-suave);
  font-weight: 550;
}
.menu-enlace[aria-current='page'] {
  color: var(--hueso);
}

@media (max-width: 760px) {
  .barra {
    display: none;
  }
  .cabecera {
    position: sticky;
    top: 0;
    z-index: 10;
    display: flex;
    align-items: center;
    justify-content: space-between;
    width: 100%;
    height: 56px;
    padding: 0 8px 0 16px;
    background: var(--noche);
    border-bottom: 1px solid var(--veta);
  }
  .cabecera .palabra {
    height: 24px;
  }
  .menu-boton {
    width: 44px;
    height: 44px;
    border: 0;
    border-radius: 999px;
    background: var(--velo);
    color: var(--hueso);
    display: grid;
    place-items: center;
    cursor: pointer;
  }
  .menu-movil {
    display: flex;
    flex-wrap: wrap;
    width: 100%;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    padding: 12px 16px;
    background: var(--piedra);
    border-bottom: 1px solid var(--veta);
  }
  .inferior {
    position: fixed;
    inset: auto 0 0;
    z-index: 10;
    display: flex;
    justify-content: space-around;
    padding: 6px 8px calc(6px + env(safe-area-inset-bottom));
    background: var(--noche);
    border-top: 1px solid var(--veta);
  }
  .inferior a {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 2px;
    flex: 1 1 0;
    min-width: 0;
    min-height: 48px;
    justify-content: center;
    font-size: 12px;
    font-weight: 550;
    color: var(--hueso-suave);
  }
  .inferior a[aria-current='page'] {
    color: var(--hueso);
  }
}
</style>
