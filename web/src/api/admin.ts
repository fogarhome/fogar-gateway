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
  ConversationLive,
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

export const getOrchestratorStatus = () => api<ServiceDetail>('/system/services/orchestrator')
/** Hace un sondeo real del transcriptor: solo al entrar en Estado y al comprobar servicios. */
export const getTranscriberStatus = () => api<ServiceDetail>('/system/services/transcriber')
export const getTtsStatus = () => api<ServiceDetail>('/system/services/tts')
export const reconnectOrchestrator = () => api<{ accepted: boolean }>('/system/services/orchestrator/reconnect', { method: 'POST' })
/** Sondea los tres servicios de verdad (puede tardar): solo al pulsar «Comprobar servicios». */
export const getReady = () => api<ReadyResponse>('/ready', { okStatuses: [503], timeoutMs: 20_000 })

const json = (body: unknown) => JSON.stringify(body)
const enc = encodeURIComponent

export const listClients = () => api<ClientResponse[]>('/home/devices')
export const getClient = (id: string) => api<ClientResponse>(`/home/devices/${enc(id)}`)
/** La única respuesta, con la de rotar, que trae la clave: no se guarda en ningún store. */
export const createClient = (body: ClientInput) => api<ClientWithKey>('/home/devices', { method: 'POST', body: json(body) })
export const updateClient = (id: string, body: ClientInput) =>
  api<ClientResponse>(`/home/devices/${enc(id)}`, { method: 'PATCH', body: json(body) })
export const deleteClient = (id: string) => api<void>(`/home/devices/${enc(id)}`, { method: 'DELETE' })
export const rotateClientKey = (id: string) => api<ClientWithKey>(`/home/devices/${enc(id)}/rotate-key`, { method: 'POST' })

export const listSessions = () => api<SessionList>('/system/sessions')
export const getSession = (id: string) => api<SessionDetail>(`/system/sessions/${enc(id)}`)

/** El código solo llega aquí: no se guarda ni se registra. */
export const openPairing = (template: PairingTemplate = {}) =>
  api<PairingOpened>('/home/pairing', { method: 'POST', body: json(template) })
export const getPairing = (id: string) => api<PairingSummary>(`/home/pairing/${enc(id)}`)
export const listPairings = () => api<PairingSummary[]>('/home/pairing')
/** `keepalive`: el DELETE sigue en vuelo aunque la pestaña se cierre (`pagehide`). */
export const cancelPairing = (id: string, keepalive?: boolean) =>
  api<void>(`/home/pairing/${enc(id)}`, { method: 'DELETE', ...(keepalive ? { keepalive } : {}) })

export const listConversations = () => api<Conversation[]>('/conversations')
/** Lo dicho, de OpenClaw: 503 `orchestrator_unavailable` si no responde, 502 `orchestrator_error` si lo rechaza. */
export const getConversationMessages = (id: string, limit = 200) =>
  api<ConversationMessages>(`/conversations/${enc(id)}/messages?limit=${limit}`, { timeoutMs: 20_000 })
export const getConversationAgents = () => api<ConversationAgents>('/conversations/agents')
export const getConversationLive = (id: string) => api<ConversationLive>(`/conversations/${enc(id)}`)
export const postMessage = (id: string, text: string, interrupt = false) =>
  api<{ message_id: string }>(`/conversations/${enc(id)}/messages`, { method: 'POST', body: json({ text, interrupt }) })
export const cancelTurn = (id: string) => api<{ cancelled: boolean }>(`/conversations/${enc(id)}/cancel`, { method: 'POST' })
export const withdrawMessage = (id: string, messageId: string) =>
  api<void>(`/conversations/${enc(id)}/messages/${enc(messageId)}`, { method: 'DELETE' })
/** Conversación nueva del panel web; sin agente, el que toque por defecto. */
export const createConversation = (agent: string | null) =>
  api<Conversation>('/conversations', { method: 'POST', body: json(agent ? { agent } : {}) })

// Personas y hogar (4.0): /home/users y /home son de dueños y administradores.
export const listUsers = () => api<UserRow[]>('/home/users')
export const createUser = (body: UserInput) => api<UserRow>('/home/users', { method: 'POST', body: json(body) })
export const updateUser = (id: string, body: UserInput) =>
  api<UserRow>(`/home/users/${enc(id)}`, { method: 'PATCH', body: json(body) })
export const deleteUser = (id: string) => api<void>(`/home/users/${enc(id)}`, { method: 'DELETE' })
export const getHome = () => api<Home>('/home')
/** `notify_client_id`: el dispositivo de avisos del hogar (`null` lo quita); 422 `invalid_notify_device` si no vale. */
export const updateHome = (body: Partial<Pick<Home, 'name' | 'agent' | 'notify_client_id'>>) =>
  api<Home>('/home', { method: 'PATCH', body: json(body) })

// Lo mío (cualquiera): quién soy, mis dispositivos con sesión y dónde suenan mis avisos.
export const getMe = () => api<Me>('/auth/me')
/** Solo uno de mis aparatos personales activos; 422 `invalid_notify_device` si no vale. */
export const updateMe = (body: { notify_client_id: string | null }) =>
  api<Me>('/auth/me', { method: 'PATCH', body: json(body) })
export const listMyDevices = () => api<MyDevice[]>('/auth/devices')
export const retireMyDevice = (id: string) => api<void>(`/auth/devices/${enc(id)}`, { method: 'DELETE' })
