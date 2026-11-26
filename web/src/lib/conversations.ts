import type { Conversation } from '@/api/types'
import { formatShortDate } from './format'

/** El título de OpenClaw o, si no lo tiene (o no responde), uno honesto con el día. */
export function conversationTitle(c: Pick<Conversation, 'title' | 'kind' | 'day' | 'created_at'>, now = new Date()): string {
  const title = c.title?.trim()
  if (title) return title
  if (c.kind === 'default' && c.day) return `Conversación del ${formatShortDate(c.day, now)}`
  return `Sin título · ${formatShortDate(c.created_at, now)}`
}

/** Qué clase de conversación es, solo cuando aporta: la del día (diaria). */
export function conversationKind(kind: string): string | null {
  if (kind === 'default') return 'diaria'
  return null
}

/** De quién es (4.0): privada de su persona o del hogar, la ven todos los de casa. */
export const conversationScope = (c: Pick<Conversation, 'owner_user_id'>) =>
  c.owner_user_id ? 'privada' : 'del hogar'
