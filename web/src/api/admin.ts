import { api } from './http'
import type {
  ClientInput,
  Home,
  Me,
  MyDevice,
  UserInput,
  UserRow,
  ClientResponse,
  ClientWithKey,
  Conversation,
  ConversationAgents,
  ConversationMessages,
  PairingOpened,
  PairingSummary,
  PairingTemplate,
  ReadyResponse,
  ServiceDetail,
  SessionDetail,
  SessionList,
} from './types'

// Una función por endpoint que usa la web.

export const getOrchestratorStatus = () => api<ServiceDetail>('/admin/orchestrator/status')
/** Hace un sondeo real del transcriptor: solo al entrar en Estado y al comprobar servicios. */
export const getTranscriberStatus = () => api<ServiceDetail>('/admin/transcriber/status')
export const getTtsStatus = () => api<ServiceDetail>('/admin/tts/status')
export const reconnectOrchestrator = () => api<{ accepted: boolean }>('/admin/orchestrator/reconnect', { method: 'POST' })
/** Sondea los tres servicios de verdad (puede tardar): solo al pulsar «Comprobar servicios». */
export const getReady = () => api<ReadyResponse>('/ready', { okStatuses: [503], timeoutMs: 20_000 })

const json = (body: unknown) => JSON.stringify(body)
const enc = encodeURIComponent

export const listClients = () => api<ClientResponse[]>('/admin/clients')
export const getClient = (id: string) => api<ClientResponse>(`/admin/clients/${enc(id)}`)
/** La única respuesta, con la de rotar, que trae la clave: no se guarda en ningún store. */
export const createClient = (body: ClientInput) => api<ClientWithKey>('/admin/clients', { method: 'POST', body: json(body) })
export const updateClient = (id: string, body: ClientInput) =>
  api<ClientResponse>(`/admin/clients/${enc(id)}`, { method: 'PATCH', body: json(body) })
export const deleteClient = (id: string) => api<void>(`/admin/clients/${enc(id)}`, { method: 'DELETE' })
export const rotateClientKey = (id: string) => api<ClientWithKey>(`/admin/clients/${enc(id)}/rotate-key`, { method: 'POST' })

export const listSessions = () => api<SessionList>('/admin/sessions')
export const getSession = (id: string) => api<SessionDetail>(`/admin/sessions/${enc(id)}`)

/** El código solo llega aquí: no se guarda ni se registra. */
export const openPairing = (template: PairingTemplate = {}) =>
  api<PairingOpened>('/admin/pairing', { method: 'POST', body: json(template) })
export const getPairing = (id: string) => api<PairingSummary>(`/admin/pairing/${enc(id)}`)
export const listPairings = () => api<PairingSummary[]>('/admin/pairing')
/** `keepalive`: el DELETE sigue en vuelo aunque la pestaña se cierre (`pagehide`). */
export const cancelPairing = (id: string, keepalive?: boolean) =>
  api<void>(`/admin/pairing/${enc(id)}`, { method: 'DELETE', ...(keepalive ? { keepalive } : {}) })

export const listConversations = () => api<Conversation[]>('/admin/conversations')
/** Lo dicho, de OpenClaw: 503 `orchestrator_unavailable` si no responde, 502 `orchestrator_error` si lo rechaza. */
export const getConversationMessages = (id: string, limit = 200) =>
  api<ConversationMessages>(`/admin/conversations/${enc(id)}/messages?limit=${limit}`, { timeoutMs: 20_000 })
export const getConversationAgents = () => api<ConversationAgents>('/admin/conversations/agents')
/** Conversación nueva del panel web; sin agente, el que toque por defecto. */
export const createConversation = (agent: string | null) =>
  api<Conversation>('/admin/conversations', { method: 'POST', body: json(agent ? { agent } : {}) })

// Personas y hogar (4.0): /admin/users y /admin/home son de dueños y administradores.
export const listUsers = () => api<UserRow[]>('/admin/users')
export const createUser = (body: UserInput) => api<UserRow>('/admin/users', { method: 'POST', body: json(body) })
export const updateUser = (id: string, body: UserInput) =>
  api<UserRow>(`/admin/users/${enc(id)}`, { method: 'PATCH', body: json(body) })
export const deleteUser = (id: string) => api<void>(`/admin/users/${enc(id)}`, { method: 'DELETE' })
export const getHome = () => api<Home>('/admin/home')
/** `notify_client_id`: el dispositivo de avisos del hogar (`null` lo quita); 422 `invalid_notify_device` si no vale. */
export const updateHome = (body: Partial<Pick<Home, 'name' | 'agent' | 'notify_client_id'>>) =>
  api<Home>('/admin/home', { method: 'PATCH', body: json(body) })

// Lo mío (cualquiera): quién soy, mis dispositivos con sesión y dónde suenan mis avisos.
export const getMe = () => api<Me>('/auth/me')
/** Solo uno de mis aparatos personales activos; 422 `invalid_notify_device` si no vale. */
export const updateMe = (body: { notify_client_id: string | null }) =>
  api<Me>('/auth/me', { method: 'PATCH', body: json(body) })
export const listMyDevices = () => api<MyDevice[]>('/auth/devices')
export const retireMyDevice = (id: string) => api<void>(`/auth/devices/${enc(id)}`, { method: 'DELETE' })
