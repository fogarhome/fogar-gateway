import type { Conversation } from '@/api/types'
import { formatShortDate } from './format'

/** El título de OpenClaw o, si no lo tiene (o no responde), uno honesto con el día. */
export function conversationTitle(c: Pick<Conversation, 'title' | 'kind' | 'day' | 'created_at'>, now = new Date()): string {
  const title = c.title?.trim()
  if (title) return title
  if (c.kind === 'default' && c.day) return `Conversación del ${formatShortDate(c.day, now)}`
  return `Sin título · ${formatShortDate(c.created_at, now)}`
}

/** Qué clase de conversación es, solo cuando aporta: la del día (diaria) y las de antes de M19. */
export function conversationKind(kind: string): string | null {
  if (kind === 'default') return 'diaria'
  if (kind === 'legacy') return 'de antes'
  return null
}

/** Se escribe solo en las conversaciones empezadas en este navegador; las demás que puedes ver se
 * leen. Abrir aquí la de otro aparato lo cortaría (4102) mientras no haya varios en una (C3). */
export const canWrite = (c: Pick<Conversation, 'client_id'>, deviceId: string | null) =>
  deviceId !== null && c.client_id === deviceId

/** De quién es (4.0): privada de su persona o del hogar, la ven todos los de casa. */
export const conversationScope = (c: Pick<Conversation, 'owner_user_id'>) =>
  c.owner_user_id ? 'privada' : 'del hogar'
