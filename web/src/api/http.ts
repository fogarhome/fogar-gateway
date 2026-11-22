import { useSessionStore } from '@/stores/session'

export type ApiErrorKind =
  | 'unauthorized'
  | 'forbidden'
  | 'not-configured'
  | 'not-found'
  | 'conflict'
  | 'invalid'
  | 'too-many'
  | 'network'
  | 'server'

export class ApiError extends Error {
  constructor(
    readonly kind: ApiErrorKind,
    readonly status: number,
    message: string,
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

function kindFor(status: number): ApiErrorKind {
  if (status === 401) return 'unauthorized'
  if (status === 403) return 'forbidden' // un miembro en lo del hogar, o el login por HTTP en la LAN
  if (status === 429) return 'too-many' // demasiados intentos de iniciar sesión
  if (status === 503) return 'not-configured'
  if (status === 404) return 'not-found'
  if (status === 409) return 'conflict'
  if (status === 422) return 'invalid'
  return 'server'
}

export interface RequestOptions extends RequestInit {
  /** Token que sustituye al guardado (null: sin token, como el inicio de sesión). */
  token?: string | null
  timeoutMs?: number
  /** Códigos que traen un cuerpo válido aunque no sean 2xx (`/ready` responde 503 si cae OpenClaw). */
  okStatuses?: number[]
}

/** Petición a la API del gateway (mismo origen) con el token de este navegador como Bearer. */
export async function api<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const { token, timeoutMs = 10_000, okStatuses = [], headers, ...init } = options
  const bearer = token === undefined ? useSessionStore().token : token
  const timeout = AbortSignal.timeout(timeoutMs)
  const signal = init.signal ? AbortSignal.any([init.signal, timeout]) : timeout

  let response: Response
  try {
    response = await fetch(path, {
      ...init,
      signal,
      headers: {
        Accept: 'application/json',
        ...(init.body ? { 'Content-Type': 'application/json' } : {}),
        ...(bearer ? { Authorization: `Bearer ${bearer}` } : {}),
        ...headers,
      },
    })
  } catch {
    throw new ApiError('network', 0, 'No llego al gateway')
  }

  if (!response.ok && !okStatuses.includes(response.status)) {
    let detail = response.statusText
    try {
      const body = (await response.json()) as { detail?: unknown }
      if (typeof body.detail === 'string') detail = body.detail
    } catch {
      // cuerpo sin JSON
    }
    throw new ApiError(kindFor(response.status), response.status, detail)
  }

  if (response.status === 204) return undefined as T
  return (await response.json()) as T
}
