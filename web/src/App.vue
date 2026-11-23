<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import BarraLateral from './components/BarraLateral.vue'
import FgAvisos from './components/FgAvisos.vue'
import { useClientsStore } from './stores/clients'
import { useLiveStore } from './stores/live'
import { api, ApiError } from './api/http'
import { type SessionUser, useSessionStore } from './stores/session'

const route = useRoute()
const router = useRouter()
const session = useSessionStore()
const live = useLiveStore()
const clients = useClientsStore()

const isPublic = computed(() => Boolean(route.meta.public))
// El nombre de la casa (de /auth/me, con cualquier rol): para saber en cuál estás.
const nombre = ref<string>()

// El estado vivo y los clientes, solo para quien cuida del hogar (dueño o administrador).
watch(
  () => session.token && session.isAdmin,
  (admin) => {
    if (admin) {
      live.start(() => void router.replace('/conectar'))
      void clients.refresh()
    } else live.halt()
  },
  { immediate: true },
)

// Al abrir con una sesión guardada: quién soy ahora (el rol puede haber cambiado) y, si el
// dispositivo se retiró o la persona se desactivó, otra vez a Conectar.
watch(
  () => session.token,
  async (token) => {
    if (!token) return
    try {
      const me = await api<{ user: SessionUser; home?: { name: string } | null }>('/auth/me')
      session.setUser(me.user)
      nombre.value = me.home?.name
      if (route.meta.admin && !session.isAdmin) await router.replace('/conversaciones')
    } catch (e) {
      if (e instanceof ApiError && e.kind === 'unauthorized') {
        session.signOut()
        await router.replace('/conectar')
      }
    }
  },
  { immediate: true },
)

</script>

<template>
  <RouterView v-if="isPublic" />
  <div v-else class="armazon">
    <BarraLateral :nombre="nombre" :version="live.state?.gateway.version" />
    <main class="contenido">
      <RouterView />
    </main>
  </div>
  <FgAvisos />
</template>

<style scoped>
.armazon {
  min-height: 100vh;
  display: flex;
  flex-wrap: wrap;
  align-items: flex-start;
}
.contenido {
  flex: 1 1 560px;
  min-width: 0;
  max-width: 1240px;
  padding: 36px 40px 64px;
}
@media (max-width: 760px) {
  .armazon {
    flex-direction: column;
    flex-wrap: nowrap;
  }
  .contenido {
    flex: 1 1 auto;
    width: 100%;
    padding: 20px 16px 96px;
  }
}
</style>
