import { defineStore } from 'pinia'
import { computed, ref } from 'vue'

// La sesión de esta web (4.0): el token de este navegador como dispositivo personal (de
// /auth/login) y quién es su persona. El token va como Bearer a /admin y /auth y como
// client_key al WebSocket; nunca se enseña.
export type Role = 'owner' | 'admin' | 'member'

export interface SessionUser {
  id: string
  username: string
  display_name: string
  role: Role
}

interface Stored {
  token: string
  /** Este navegador como dispositivo (su `client_id`): sus conversaciones se escriben aquí. */
  deviceId: string
  user: SessionUser
}

const KEY = 'fogar.session'
const OLD_KEY = 'fogar.adminToken' // la 3.x guardaba aquí el ADMIN_TOKEN: ya no sirve

// El almacenamiento del navegador puede fallar (ventana privada, datos bloqueados):
// cada acceso va protegido y la web sigue funcionando sin recordar nada.
function read(): Stored | null {
  try {
    localStorage.removeItem(OLD_KEY)
    sessionStorage.removeItem(OLD_KEY)
    const raw = localStorage.getItem(KEY) ?? sessionStorage.getItem(KEY)
    const parsed = raw ? (JSON.parse(raw) as Partial<Stored>) : null
    return parsed?.token && parsed.user && parsed.deviceId ? (parsed as Stored) : null
  } catch {
    return null
  }
}

function write(value: Stored, remember: boolean): void {
  try {
    ;(remember ? localStorage : sessionStorage).setItem(KEY, JSON.stringify(value))
    ;(remember ? sessionStorage : localStorage).removeItem(KEY)
  } catch {
    // sin almacenamiento: la sesión vive solo en memoria
  }
}

function forget(): void {
  try {
    localStorage.removeItem(KEY)
    sessionStorage.removeItem(KEY)
  } catch {
    // nada que borrar
  }
}

export const useSessionStore = defineStore('session', () => {
  const stored = read()
  const token = ref<string | null>(stored?.token ?? null)
  const user = ref<SessionUser | null>(stored?.user ?? null)
  const deviceId = ref<string | null>(stored?.deviceId ?? null)
  let remembered = (() => {
    try {
      return localStorage.getItem(KEY) !== null
    } catch {
      return false
    }
  })()

  // Dueño o administrador: el hogar entero. Un miembro, lo suyo y lo común.
  const isAdmin = computed(() => user.value?.role === 'owner' || user.value?.role === 'admin')

  function signIn(newToken: string, newDeviceId: string, newUser: SessionUser, remember: boolean): void {
    token.value = newToken
    deviceId.value = newDeviceId
    user.value = newUser
    remembered = remember
    write({ token: newToken, deviceId: newDeviceId, user: newUser }, remember)
  }

  /** Lo que diga /auth/me (el rol puede haber cambiado desde otro sitio). */
  function setUser(newUser: SessionUser): void {
    user.value = newUser
    if (token.value && deviceId.value) write({ token: token.value, deviceId: deviceId.value, user: newUser }, remembered)
  }

  function signOut(): void {
    token.value = null
    deviceId.value = null
    user.value = null
    forget()
  }

  return { token, deviceId, user, isAdmin, signIn, setUser, signOut }
})
