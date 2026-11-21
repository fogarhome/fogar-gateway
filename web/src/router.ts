import { createRouter, createWebHistory } from 'vue-router'
import { useSessionStore } from './stores/session'

export const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'estado', component: () => import('./views/EstadoView.vue'), meta: { admin: true } },
    { path: '/conversaciones', name: 'conversaciones', component: () => import('./views/ConversacionesView.vue') },
    { path: '/conversaciones/:id', name: 'conversacion', component: () => import('./views/ConversacionView.vue') },
    { path: '/sesiones/:id?', name: 'sesiones', component: () => import('./views/SesionesView.vue'), meta: { admin: true } },
    { path: '/clientes', name: 'clientes', component: () => import('./views/ClientesView.vue'), meta: { admin: true } },
    { path: '/clientes/:id', name: 'cliente', component: () => import('./views/ClienteView.vue'), meta: { admin: true } },
    { path: '/personas', name: 'personas', component: () => import('./views/PersonasView.vue'), meta: { admin: true } },
    { path: '/dispositivos', name: 'dispositivos', component: () => import('./views/DispositivosView.vue') },
    {
      path: '/conectar',
      name: 'conectar',
      component: () => import('./views/ConectarView.vue'),
      meta: { public: true },
    },
    { path: '/:pathMatch(.*)*', redirect: '/' },
  ],
})

// Sin sesión no hay nada que enseñar: todo pasa por Conectar. Lo del hogar entero (estado,
// sesiones, clientes, personas) es de dueños y administradores; un miembro va a Conversaciones.
router.beforeEach((to) => {
  const session = useSessionStore()
  const inicio = session.isAdmin ? { name: 'estado' } : { name: 'conversaciones' }
  if (!to.meta.public && !session.token) return { name: 'conectar' }
  if (to.name === 'conectar' && session.token) return inicio
  if (to.meta.admin && !session.isAdmin) return { name: 'conversaciones' }
  return true
})
