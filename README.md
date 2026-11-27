# Fogar Gateway

La puerta de entrada de Fogar en casa. Un solo servicio que reconoce cada aparato del hogar, lo conecta con el asistente y con la voz, y se administra desde el navegador o la terminal.

- **Aparatos**: Fogar TV, el cliente de voz (Mac y Android), la app de iPhone y Home Assistant. Un aparato nuevo **descubre el gateway y se empareja con un código de 6 cifras**, sin copiar claves.
- **Asistente**: los turnos de voz y texto van a [OpenClaw](docs/openclaw-protocol.md), que elige el agente y sus herramientas. El gateway lleva el turno de principio a fin: streaming, cola de mensajes, «parar» y «enviar ahora», y avisos que llegan sin preguntar (push). Hablar encima no corta nada: pausar o callar la voz lo decide el cliente (5.0).
- **Voz**: transcripción (STT) y síntesis (TTS) con los servicios propios de Fogar, con ElevenLabs o desactivadas. Sin voz, el texto sigue funcionando.
- **Personas** (4.0): cada persona de casa entra con su cuenta. Lo que habla desde sus dispositivos es suyo; lo que se habla desde la tele o los altavoces es del hogar. Dueño, administradores y miembros, como en Home Assistant.
- **Administración**: un panel web en `/` (estado en vivo, conversaciones y chat con Fogar, sesiones, clientes y personas) y la CLI `fogar-gw` (diagnóstico, configuración, servicio, logs, emparejamiento y personas).
- **Instalación**: un comando instala, verifica OpenClaw y la voz contra los servicios reales, lo deja como servicio de usuario y te dice cómo llegar al panel: desde casa, por Tailscale o por tu propio dominio.

```
  Aparatos de casa                    Fogar Gateway  :8004                     Servicios
  ────────────────                    ────────────────────                     ─────────
  Fogar TV           ──┐          ┌── /ws/stream      voz y texto en vivo  ──┐
  Voz (Mac, Android) ──┤ descubren│   /.well-known    descubrimiento        │──► OpenClaw (agentes)
  App iPhone         ──┤ y se     │   /pair           emparejamiento        │──► STT: propio · ElevenLabs · off
  Home Assistant     ──┘ emparejan└── /v1/*           API compatible OpenAI ┘──► TTS: propio · ElevenLabs · off

  Tú, desde casa, Tailscale o tu dominio:
    navegador ──► /           panel web, con tu cuenta (/auth): conversaciones y, si cuidas del hogar, el resto
    terminal  ──► fogar-gw    CLI sobre /home y /system con ADMIN_TOKEN (doctor, config, servicio, logs, pair, users)
```

La identidad y la configuración de cada aparato viven en una base de datos SQLite local (`<base>/data/gateway.db`, con `<base>` = `~/.fogar-gateway` por defecto). Requiere OpenClaw **2026.6.11 o posterior**.

```bash
curl -fsSL https://github.com/fogarhome/fogar-gateway/releases/latest/download/install.sh | sh
```

Detalles en [Instalación](#instalación). Si integras un aparato nuevo, el protocolo está en [`docs/client-protocol.md`](docs/client-protocol.md).

---

## Endpoints

### Web

`http://<host>:8004/` es el panel de Fogar: cada persona entra con su usuario y contraseña, y desde ahí se ve el estado en vivo (también con qué acceso está el gateway en OpenClaw), las conversaciones que puede ver y las sesiones, se habla con Fogar por texto en cualquiera de ellas (5.0: la misma conversación puede estar abierta en varios aparatos a la vez), y se emparejan, crean, editan, rotan y retiran aparatos. Lo sirve el propio gateway desde su paquete, sin Node en la máquina; las rutas de la API de abajo tienen siempre prioridad. Si el paquete no lleva la web compilada (un clon sin build), `/` da 404, el gateway funciona igual y lo avisa una vez en el log. **Desde un clon**, `sh scripts/build-web.sh` (en la raíz; necesita Node y pnpm) la compila y la copia a `src/fogar_gateway/web/`, que es lo que sirve el gateway (con `uv sync` el paquete es editable: basta con reiniciarlo). `pnpm build` solo la deja en `web/dist`, que el gateway no mira. Es el mismo script que usan la release, la CI y el Dockerfile. Para desarrollarla, [`web/README.md`](web/README.md).

### WebSocket

| Endpoint | Auth | Descripción |
|----------|------|-------------|
| `ws://<host>:8004/ws/stream` | `token` en handshake JSON (permiso `conversations:write`) | Sesión interactiva de voz/texto |
| `ws://<host>:8004/speech/dictation` | `token` en handshake JSON (permiso `conversations:write`) | Solo dictado (5.0): audio → transcripciones; no inicia turnos |

Ver [`docs/client-protocol.md`](docs/client-protocol.md) para la guía completa de integración. Para chatear sin WebSocket (`/events` y `POST`), su §16.

### Health

| Endpoint | Método | Auth | Descripción |
|----------|--------|------|-------------|
| `/healthz` | GET | ninguna | Liveness — siempre 200 si el proceso vive |
| `/ready` | GET | ninguna | Readiness — 200 ok/degraded, 503 si OpenClaw no responde. Un proveedor de voz en `off` sale `unavailable` en `services` pero no degrada `status` |

```json
// GET /ready — ejemplo degradado
{
  "status": "degraded",
  "services": {
    "orchestrator": "ok",
    "transcriber": "unavailable",
    "tts": "ok"
  }
}
```

### Descubrimiento y emparejamiento (públicos)

| Endpoint | Método | Auth | Descripción |
|----------|--------|------|-------------|
| `/.well-known/fogar-gateway` | GET | ninguna | Identifica al gateway: `{"service": "fogar-gateway", "version", "instance_id"}` (sin nombre: privacidad; `instance_id` = identificador aleatorio por arranque del proceso). El nombre (`GATEWAY_NAME`) llega al canjear un código en `POST /pair`, en `gateway.name` |
| `/pair` | POST | un código de `fogar-gw pair` | El cliente canjea el código (y pide sus `scopes`) y recibe su `token`, la única vez que se ve, y los `scopes` concedidos. Ver [Emparejar un cliente](#emparejar-un-cliente) |

Los dos son públicos: detrás de un proxy o un túnel también llegan desde internet. `/pair` solo acepta un código abierto por el administrador, y 5 códigos erróneos seguidos anulan todos los abiertos. El protocolo para clientes está en [`docs/client-protocol.md` §0](docs/client-protocol.md#0-descubrir-el-gateway-y-emparejarse).

### Personas y sesiones (`/auth`)

**El hogar y sus personas.** Una instalación es un hogar. Cada persona tiene una cuenta con un rol: **dueño** (siempre hay al menos uno activo), **administrador** (el hogar entero, salvo tocar a un dueño o dar ese rol) o **miembro** (lo suyo y lo común). Hay dos clases de dispositivo:

- **Del hogar**: los que se emparejan con un código (la tele, los altavoces, Home Assistant). No se sabe quién habla: lo que se dice desde ellos es **del hogar**, y desde ellos solo se ven las conversaciones del hogar.
- **Personales**: cada inicio de sesión (la web, una app). Lo que se dice desde ellos es **privado** de su persona, y desde ellos se ven las suyas y las del hogar. Nadie lee las privadas de otro, tampoco un dueño; solo la CLI con `ADMIN_TOKEN`, que es la máquina.

La privacidad es **dentro de Fogar**: OpenClaw, su panel y sus otros canales (Telegram…) ven todas las sesiones. Cada persona habla con su agente o, si no tiene, con el de lo común (`/home`); si varias comparten agente, comparten su memoria aunque las conversaciones sean privadas, y la web lo avisa.

| Endpoint | Método | Auth | Descripción |
|----------|--------|------|-------------|
| `/auth/login` | POST | usuario y contraseña | `{"username", "password", "device_name", "client_type"?, "capabilities"?, "scopes"?}` → `201 {"client_id", "token", "scopes", "user", "home"}`: este aparato pasa a ser un dispositivo personal y `token` es su credencial, la única vez que se ve; `scopes`, lo pedido ∩ el tope del rol (sin pedir, `conversations:write`). Errores: `401 invalid_credentials` (siempre el mismo), `403 insecure_transport`, `429 too_many_attempts` con `Retry-After` |
| `/auth/me` | GET | Bearer | `{"user": {id, username, display_name, role}, "home", "device", "scopes", "notify_client_id"}`; `scopes`, los efectivos de este aparato; `notify_client_id` es su dispositivo de avisos (`null` = ninguno) |
| `/auth/me` | PATCH | Bearer | `{"notify_client_id": id \| null}`: elige su dispositivo de avisos, solo uno de sus aparatos personales activos (un aviso privado nunca suena en un aparato del hogar ni de otra persona); `null` lo quita. Responde como `GET`. `422 invalid_notify_device` si no vale |
| `/auth/devices` | GET | Bearer | Mis dispositivos con sesión (`current` marca este; `is_active` falso si un administrador lo desactivó; `push_enabled` y `output_mode`, lo que necesita para sonar un aviso) |
| `/auth/devices/{id}` | DELETE | Bearer | Retira uno de los míos: su token deja de valer y sus sesiones se cierran ya (4200) |
| `/auth/logout` | POST | Bearer | Retira el dispositivo de esta petición |

El token va como `Authorization: Bearer <token>` por HTTP y como `token` en el WebSocket: una sola credencial por aparato, personal o del hogar (la clave de un emparejado también vale como Bearer). **Permisos (5.0):** cada aparato tiene `scopes` (`conversations:read`, `conversations:write`, `home:read`, `home:admin`; `write` y `admin` incluyen su `read`); vale lo concedido ∩ su tope, que es el rol de su persona (miembro: solo `conversations:*`) o, en uno del hogar, como mucho `home:read`. Falta uno → `403 missing_scope` con `WWW-Authenticate: Bearer error="insufficient_scope", scope="…"`; en `/ws/stream`, cierre 4011. Detalle: [`docs/client-protocol.md` §0](docs/client-protocol.md#permisos-scopes-50). Es opaco, se guarda solo su SHA-256 y se busca en la base de datos en cada petición: no caduca ni se refresca, y retirar el dispositivo o desactivar a la persona lo corta al momento. **La contraseña solo viaja cifrada**: el login se acepta por HTTPS (también detrás de `tailscale serve` o `cloudflared` en esta máquina), y por HTTP solo desde esta misma máquina o la tailnet; desde la LAN por HTTP responde `403 insecure_transport` (pon HTTPS con `fogar-gw access setup`). Tras 5 fallos seguidos, por IP o por cuenta, la espera se dobla con cada uno (hasta 5 min). Quitar un dispositivo o cerrar sesión lo **retira**, no lo borra: sus conversaciones conservan su hogar y su dispositivo.

Las personas se gestionan desde la web (Personas) o con `fogar-gw users`. **Si el dueño pierde la contraseña**, la recuperación es en la máquina del gateway: `fogar-gw users reset-password USUARIO`.

### Hogar, sistema y conversaciones (5.0)

`/admin` ya no existe: cada cosa va bajo su competencia (`/home`, `/system`, `/conversations`). Quien llama: la máquina (`X-Admin-Token`, puede todo) o un aparato con su Bearer y el scope de la columna. En `/home` y `/system` leer es `home:read` y escribir, `home:admin`; `/home/pairing` es todo `home:admin`. Las conversaciones piden `conversations:read` (leer) o `conversations:write` (crear), y además cada aparato solo ve las que le tocan (las del hogar y, si es personal, las de su persona).

| Antes (4.x) | Ahora |
|---|---|
| `/admin/clients…` | `/home/devices…` |
| `/admin/users…`, `/admin/home` | `/home/users…`, `/home` |
| `/admin/pairing…` | `/home/pairing…` |
| `/admin/state…`, `/admin/sessions…`, `/admin/access`, `/admin/speech` | `/system/state…`, `/system/sessions…`, `/system/access`, `/system/speech` |
| `/admin/orchestrator/status`, `/admin/transcriber/status`, `/admin/tts/status`, `/admin/orchestrator/reconnect` | `/system/services/orchestrator`, `…/transcriber`, `…/tts`, `POST /system/services/orchestrator/reconnect` |
| `/admin/conversations…` | `/conversations…` |


| Endpoint | Método | Descripción |
|----------|--------|-------------|
| `/system/sessions` | GET | Sesiones activas y recientes en memoria |
| `/system/sessions/{id}` | GET | Detalle con eventos y latencias por turno |
| `/system/services/orchestrator` | GET | Estado del orquestador (CONNECTED / RECONNECTING) |
| `/system/services/orchestrator/reconnect` | POST | Reintenta ya (corta la espera del backoff) — responde 202 con `{"accepted": true}`; `{"accepted": false}` si ya está CONNECTED |
| `/system/services/transcriber` | GET | Estado del proveedor STT activo (`provider`; con fogar, reachability en vivo; con ElevenLabs, salud cacheada + `counters`) |
| `/system/services/tts` | GET | Estado del proveedor TTS activo (`provider`, breaker, último error; con ElevenLabs, `counters`) |
| `/system/speech` | GET | Proveedores de voz activos (`stt`, `tts`) y puerta de audio efectiva |
| `/home/devices` | GET | Lista clientes (sin `client_key`; la huella `key_fp`), con `user_id` (`null` = del hogar), `capabilities` y `last_seen_at`. Un inicio de sesión también es un cliente |
| `/home/devices` | POST | Crear cliente; la respuesta trae su `client_key`, la única vez que se ve (se genera, o se importa una existente de ≥ 32 caracteres). `scopes` (por defecto `conversations:write`; un aparato del hogar como mucho `home:read`, si no `422 scope_not_allowed`); `PATCH` los cambia (a uno personal solo se le pueden quitar) |
| `/home/devices/{id}` | GET | Detalle de un cliente |
| `/home/devices/{id}` | PATCH | Actualizar cliente (campos parciales) |
| `/home/devices/{id}` | DELETE | Retirar cliente: su clave deja de valer y sus sesiones se cierran ya (4200); sigue en la BD con sus conversaciones y no sale en la lista |
| `/home/devices/{id}/rotate-key` | POST | Rotar `client_key` (la respuesta trae la nueva) |
| `/home/pairing` | POST | Abrir un código de emparejamiento con la plantilla del cliente (los campos de `POST /home/devices`, nombre opcional y sin clave). La respuesta trae el código, la única vez que se ve. Como mucho 5 abiertos (409) |
| `/home/pairing` | GET | Códigos abiertos (sin el código) |
| `/home/pairing/{id}` | GET | Estado de un código: `open`, `used` (con `client_id` y `client_name`), `expired` o `cancelled` |
| `/home/pairing/{id}` | DELETE | Anular un código abierto |
| `/system/state` | GET | Instantánea viva: versión, uptime, flujos `/system/state/stream` abiertos (`gateway.dashboards`), estado de servicios (el de OpenClaw, con su acceso: `scopes`, `admin`, `auth_method`, `pairing`, `pairing_request_id`, `identity_error`, `device_id` y, para el panel, `approve_command` con el emparejamiento pendiente e `identity_error_text` con `identity_required`; nunca secretos), contadores y una fila por sesión |
| `/system/state/stream` | GET | La misma instantánea por SSE cada segundo; cada stream termina solo a los 25 s, el cliente debe reconectar (`EventSource` lo hace solo) |
| `/conversations` | GET | Cualquier aparato con `conversations:read`: las que puede ver, las del hogar y las suyas; la máquina, todas (filtro `?client_id=`), la última en hablarse primero: `id`, `client_id`, `client_name`, `agent`, `kind` (`default`, `created`), `day`, `created_at`, `last_turn_at`, `turn_count`, `owner_user_id` (`null` = del hogar) y `title`, que se lee de OpenClaw (`sessions.list`, en caché 20 s, y un fallo, 5 s; `null` si no lo tiene o no responde; la lista nunca falla por eso). No salen las del día en las que nunca se habló (un cliente que conectó y calló) |
| `/conversations/{id}/messages` | GET | Con `conversations:read`, de una que pueda ver (si no, 404). Lo dicho, de OpenClaw (`chat.history`), normalizado a `{role, text, timestamp}` (solo `user` y `assistant`); `?limit=` 1–1000 (200). `503 orchestrator_unavailable` con OpenClaw caído, `502 orchestrator_error` si lo rechaza |
| `/conversations` | POST | Con `conversations:write`: conversación nueva de su dispositivo (privada si es personal; `{"agent"?}`; sin agente, el de la cascada). El agente se valida como en el handshake (`422 agent_not_permitted`/`agent_not_available`). La máquina no tiene dispositivo: `403 user_required` |
| `/conversations/agents` | GET | Con `conversations:read`, para el selector de la web: `{"agents", "default"}`, los agentes del roster de OpenClaw que su dispositivo puede usar (su `allowed_agents`; sin roster, esa lista o `[]`) y el que le tocaría sin elegir (`null` si ese no le está permitido). La máquina, sin política de ningún dispositivo |
| `/conversations/{id}` | GET | Con `conversations:read`: la conversación, su turno en curso (con el texto que lleva), sus mensajes pendientes y `as_of`, el último id de `/events` que incluye (5.0) |
| `/conversations/{id}/messages` | POST | Con `conversations:write`: `{"text", "interrupt"?}` → `202 {"message_id"}`; lo demás llega por `/events` |
| `/conversations/{id}/messages/{message_id}` | DELETE | Quita un mensaje pendiente (`204`; `404 not_pending`) |
| `/conversations/{id}/cancel` | POST | Para el turno en curso para todos (`{"cancelled": bool}`) |
| `/events` | GET | SSE del aparato (`conversations:read`): `ready`, luego todo lo de sus conversaciones visibles y sus avisos, con `id` y reanudación por `Last-Event-ID` (o `resync`). Ver [`docs/client-protocol.md` §16](docs/client-protocol.md#16-chat-por-http-events-y-mensajes-50) |
| `/conversations/{id}/messages/{message_id}/speech` | POST | Voz de una respuesta ya terminada (`conversations:write`): PCM16 24 kHz en streaming; `503 tts_unavailable` sin TTS (5.0) |
| `/speech/dictation` | WS | Dictado (`conversations:write`): `{"protocol": 5, "token"}`, audio PCM float32 16 kHz y `{"type":"end"}` → `transcription_partial`/`transcription`; no inicia turnos. Con `speak: true` en un mensaje, la voz de su respuesta llega por `/events` (y la de los avisos con `/events?audio=1`). Ver [`docs/client-protocol.md` §16](docs/client-protocol.md#voz-sin-el-websocket-de-conversación-50) |
| `/home/users` | GET | Personas del hogar: `id`, `username`, `display_name`, `role`, `agent` (el suyo; `null`, el de lo común), `is_active`, `created_at`, `effective_agent` y `agent_shared` |
| `/home/users` | POST | Alta: `{"username", "display_name", "password" (8+), "role"?, "agent"?}`. `409 username_taken` |
| `/home/users/{id}` | PATCH | `display_name`, `role`, `agent` (`null` vuelve al de lo común), `is_active`. Desactivar corta ya sus dispositivos |
| `/home/users/{id}/password` | PUT | Contraseña nueva (`{"password"}`); las sesiones abiertas siguen |
| `/home/users/{id}` | DELETE | Borra a la persona, sus dispositivos y sus conversaciones privadas del gateway (en OpenClaw quedan hasta que se limpie su agente) |
| `/home` | GET, PATCH | El hogar: `name`, `agent`, el de lo común (`null` = la cascada de siempre), y `notify_client_id`, su dispositivo de avisos: cualquier aparato activo del hogar, del hogar o personal (`null` = ninguno; `422 invalid_notify_device` si no vale). Retirar un aparato lo quita de donde estuviera elegido; desactivarlo, no |
| `/system/access` | GET | Vías para llegar al gateway (`public`, `tailscale_https`, `tailscale`, `lan`), cada una con su estado (`ok`, `protegida`, `otro_gateway`, `inalcanzable`, `sin_verificar`) y la recomendada; `{"docker": bool, "routes": [...]}`, en caché 15 s |

En `/home/users`, siempre queda un dueño activo (`409 last_owner`) y solo un dueño o la máquina tocan a un dueño o dan ese rol (`403 owner_only`); lo mismo vale en `/home/devices` para editar, retirar o rotar la clave de un dispositivo personal de un dueño. Los tres de `/system/services/*` comparten la forma `{"name", "state", "connected_at", "reconnect_attempts", "last_error"}`; los de transcriber y TTS añaden `provider` y, con ElevenLabs, `counters`.

**Conversaciones (M19).** Cada sesión habla en una conversación, que es una sesión de OpenClaw (`agent:{agente}:fogar-{id}`). Quien no elige cae en la del día (una por cliente, agente y día; el día empieza a las `CONVERSATION_DAY_START_HOUR`, y una sesión abierta días pasa a la nueva en su primer turno tras el corte); un cliente puede elegir una de las suyas o crear una (`conversation` en el handshake). El gateway solo guarda el índice (tabla `conversations`, sin texto): lo dicho y el título viven en OpenClaw. Cada conversación es del hogar o privada de una persona, según el dispositivo que la crea (4.0): un dispositivo del hogar abre solo las del hogar; uno personal, también las de su usuario. La web inicia sesión (`/auth/login`) y entra en el WebSocket con su `token`, como cualquier dispositivo. Quitar un cliente lo **retira** (4.0): su clave deja de valer y sus conversaciones se conservan, siempre con su hogar y su dispositivo; la base de datos lo garantiza con claves foráneas (el gateway activa `PRAGMA foreign_keys` en cada conexión). Protocolo: [`docs/client-protocol.md` §14](docs/client-protocol.md#14-conversaciones).

Los campos de un cliente (`POST`/`PATCH /home/devices`) están en `src/fogar_gateway/models/admin_schemas.py` y sus defaults en `src/fogar_gateway/db/models.py`. Desde la terminal, `fogar-gw`. Los comandos de clientes y de estado necesitan el gateway en marcha y pasan por esta API (la CLI nunca escribe en la BD); `doctor` y `config` funcionan también con el gateway parado:

```bash
fogar-gw                        # en una terminal: menú interactivo; en un script: la ayuda
fogar-gw init                   # instalación nueva: <base>, config.env con ADMIN_TOKEN aleatorio y la BD
fogar-gw onboard [--answers FICHERO] [--no-service]   # asistente: OpenClaw, voz, servicio y dashboard
fogar-gw token                  # el token de la máquina (ADMIN_TOKEN: la CLI y la recuperación), solo en una terminal
fogar-gw access                 # desde dónde se llega al gateway, verificado, con la vía recomendada
fogar-gw access setup           # elige cómo entrar (Tailscale o tu dominio) y después lista las vías
fogar-gw serve [--create-db]    # arranca el gateway (puerto 8004, host GATEWAY_HOST)
fogar-gw doctor                 # diagnóstico: configuración, BD, permisos, candado, gateway y servicios
fogar-gw config show            # ajustes efectivos y su origen (entorno, config.env o por defecto)
fogar-gw config get <AJUSTE> | validate
fogar-gw config set <AJUSTE> [VALOR|-]   # escribe en config.env; unset <AJUSTE> lo quita
fogar-gw install [--exe RUTA]   # servicio de usuario de systemd: lo instala y lo arranca
fogar-gw start|stop|restart     # restart pregunta; en scripts: fogar-gw --yes restart
fogar-gw uninstall              # quita el servicio (pregunta); no toca los datos
fogar-gw migrate --from-docker DIR   # copia una instalación Docker (.env y BD) a la nativa
fogar-gw logs [-f] [-n N] [--level NIVEL] [--session ID]
fogar-gw status                 # gateway, versión, orquestador, STT, TTS y sesiones
fogar-gw version
fogar-gw clients list           # nombre, id corto, tipo, activo, agente y huella fp
fogar-gw clients show <ref>     # <ref>: id, prefijo de id (≥ 4) o nombre
fogar-gw clients add --name … [--type ha|esp32|web|app|screen] [--agent …] [--allowed-agent …] [--import-key]
fogar-gw clients edit <ref> [--name …] [--set tts_speed=1.2 …] [--set output_mode:='["text"]']
fogar-gw clients enable|disable <ref>
fogar-gw clients rotate-key|remove <ref>    # preguntan; en scripts: fogar-gw --yes clients remove <ref>
fogar-gw pair [--name …] [--type …] [--agent …] [--set …] [--no-wait]   # código para que un cliente se dé de alta solo
fogar-gw users list             # personas: rol, agente (* si lo comparte) y si está activa
fogar-gw users add USUARIO [--name …] [--role owner|admin|member] [--agent …]   # la contraseña, oculta o por stdin
fogar-gw users edit USUARIO [--name …] [--role …] [--agent …|--common-agent]
fogar-gw users enable|disable|remove USUARIO
fogar-gw users reset-password USUARIO   # la recuperación del dueño, en la máquina del gateway
```

Opciones globales: van **antes** del subcomando (`fogar-gw --yes clients remove X`; después, click no las reconoce): `--home` (`FOGAR_GATEWAY_HOME`, por defecto `~/.fogar-gateway`), `--url` (`FOGAR_GW_URL`, por defecto `http://127.0.0.1:8004`), `--token` (`FOGAR_GW_TOKEN`; si no, `ADMIN_TOKEN` o `config.env`), `--json` y `--yes`. `FOGAR_GATEWAY_HOME` la leen el gateway y la CLI; `FOGAR_GW_URL` y `FOGAR_GW_TOKEN`, solo la CLI. Códigos de salida: 0 bien, 1 error, 2 uso incorrecto, 3 gateway inalcanzable.

- **Configuración que leen la CLI y el gateway:** variables del proceso → `<base>/config.env` → valores por defecto, con `<base>` = `--home`, `FOGAR_GATEWAY_HOME` o `~/.fogar-gateway`. Nunca el `.env` del directorio actual. Una `DATABASE_URL` relativa se resuelve contra `<base>`. Los secretos (`*_TOKEN`, `*_KEY`) salen siempre enmascarados, también en `--json`.
- **`--set`:** `campo=valor` manda texto (el servidor convierte números y booleanos); `campo:=json` manda JSON (listas, `null`). Un campo desconocido sale con 2 y lista los editables.
- **`config set`:** valida el valor de ese ajuste antes de escribir y rechaza saltos de línea (también `\f`, `\v` y los separadores de línea Unicode) y caracteres nulos; si después la configuración tiene fallos (en ese u otro ajuste), los avisa sin deshacer el cambio, antes de ofrecer el reinicio (`problems` en `--json`, y el aviso en stderr). Los fallos se juzgan como los verá el servicio: solo `config.env`, sin las variables de tu shell (p. ej. una `ELEVENLABS_API_KEY` exportada no cuenta). Los secretos (`*_TOKEN`, `*_KEY`) nunca van como argumento, para que no queden en el historial: se piden por prompt oculto (`fogar-gw config set ADMIN_TOKEN`) o, en scripts, se leen de un fichero por stdin (`fogar-gw config set ADMIN_TOKEN - < fichero`); un `echo` dejaría el secreto en el historial. Si el servicio de esta base está activo, ofrece reiniciarlo (como `fogar-gw restart`: espera a `/healthz` y sale con 1 si no arranca, con el cambio ya guardado); si no, el cambio se aplica al reiniciar (`fogar-gw restart`). Con un fallo que impediría arrancar (un ajuste inválido, o la voz elegida sin su clave o su voz) no lo reinicia, ni con `--yes`: corrígelo y reinicia después. Un `ADMIN_TOKEN` corto o redes de confianza demasiado amplias se avisan pero no lo impiden. `config unset` también quita ajustes retirados que el log pide borrar (p. ej. `ORCHESTRATOR_RECONNECT_MAX_DURATION`). Avisa si la variable está también en el entorno, porque esa manda sobre `config.env`.
- **`--import-key`** lee la clave de stdin, nunca de un argumento.
- **En Docker:** `docker exec fogar_gateway fogar-gw …`. La imagen fija `FOGAR_GATEWAY_HOME=/app`, así que `doctor` encuentra la BD del volumen. Sin `-t` no hay terminal: no se abre el menú y `rotate-key`/`remove` necesitan `--yes` (o usa `docker exec -it`). Con `GATEWAY_HOST` en una IP de la LAN, pasa `--url http://<esa IP>:8004`.

Auth: `X-Admin-Token` = `ADMIN_TOKEN` (la CLI, en la máquina del gateway) o `Authorization: Bearer <token>` de un aparato con `home:admin` (solo puede tenerlo uno personal de un **dueño o administrador** que lo pidió); sin él, `403 missing_scope`. `/conversations` pide `conversations:read` (listar, historial, agentes) o `conversations:write` (crear). Sin credenciales o con un token que no vale → `401`. Si llega `X-Admin-Token`, manda él aunque haya Bearer. `ADMIN_TOKEN` vacío y `X-Admin-Token` presente → 503.

**Límites de toda la API HTTP:** un cuerpo de más de 1 MiB recibe `413 {"detail": "request body too large"}` antes de cualquier validación o autenticación (cuenta los bytes recibidos, también sin `Content-Length`). Un `422` lista campo, tipo y mensaje de cada error, nunca el valor recibido. No hay `/docs`, `/redoc` ni `/openapi.json`: la referencia es esta página.

### OpenAI-compatible (`/v1/*`, la puerta estándar)

Cualquier herramienta que hable la API de OpenAI (Home Assistant, un script, otra app) entra por `/v1` con la clave de un aparato y habla en una conversación de Fogar que los demás aparatos ven.

**Autenticación (5.0, `resolve_v1_caller` en `src/fogar_gateway/api/openai_routes.py`):** siempre `Authorization: Bearer <token>` de un aparato con `conversations:write`, el mismo token que su handshake WebSocket. Sin Bearer, o con uno que no vale → `401`; sin el permiso → `403 missing_scope`; con la configuración del aparato ilegible en la BD (`allowed_agents` corrupto, solo editándola a mano) → `500 internal_error`. **Ninguna IP exime** (fuera `TRUST_LOOPBACK` y `TRUSTED_NETWORKS`): da igual que llegue de loopback, de la LAN o tras un proxy. Para Home Assistant, crea un aparato con `fogar-gw clients add --name "Home Assistant" --type ha` y pega el `token` que enseña como API key en la integración (Home Assistant no puede canjear un código de `fogar-gw pair`). La clave de Home Assistant de la 4.x sigue valiendo.

- **Conversación:** la cabecera `X-Fogar-Conversation: <id>` elige una que el aparato pueda ver (si no, `404 {"error": "conversation_not_found"}`), y fija el agente: un `model` que pide otro es `409 {"error": "conversation_agent_mismatch"}`. Sin la cabecera, la **conversación por defecto del día** del aparato para ese agente (M19), como un cliente WebSocket que no elige.
- `model` selecciona el agente (sin distinguir mayúsculas) y se aplican `default_agent`/`allowed_agents` del aparato (violación → `403`; un `model` que no es un id de agente válido, con `:` o espacios, es `403 agent_not_available` aunque OpenClaw no esté conectado). Sin `model`, o con el alias `fogar-gateway`, la cascada: `default_agent` del aparato, `DEFAULT_AGENT`, el agente por defecto de OpenClaw y `main`.
- `TRUSTED_PROXIES` (por defecto `127.0.0.1,::1`) ya solo decide qué IP sale en los logs: la de `X-Real-IP` si el peer es uno de esos proxies, y ninguna (con el peer entre paréntesis) si el proxy no la manda bien o hay cabeceras de túnel. Una entrada inválida o demasiado amplia sale como WARNING al arrancar y en `fogar-gw config validate`/`doctor`.
- Host de escucha de `fogar-gw serve` (también en Docker): `GATEWAY_HOST` (default `0.0.0.0`); detrás de un proxy en el mismo host usa `GATEWAY_HOST=127.0.0.1`.
- `HANDSHAKE_MAX_BYTES` mide **caracteres**, no bytes (con caracteres multibyte el tope real puede ser hasta 4x mayor). El tope de 1 MiB por mensaje WebSocket lo pone `fogar-gw serve`: con `uvicorn` a mano rige el de uvicorn (16 MiB).
- **`/v1` pasa por el canal de su conversación** (como un mensaje del WebSocket): las sesiones que tengan abierta esa conversación ven su mensaje y su turno (con el aparato como origen, sin audio). Su respuesta es siempre la de su propio run: si hay un turno en marcha en la conversación, la petición espera a que termine (y lo que llegue mientras corre el suyo espera detrás), así que dos peticiones a la vez ya no reciben `409 turn_in_progress`: la segunda espera a la primera. Esa espera tiene tope: si en `TURN_TIMEOUT_S` la conversación no queda libre, `502 turn_timeout` sin enviar nada. Si otro aparato para su turno (`cancel` o un `send` con `interrupt`, que se atienden en el acto aunque ese aparato tenga envíos esperando), `502 turn_cancelled`; si OpenClaw incorpora su mensaje a un turno que empezó a la vez (un push), `502 turn_absorbed`, nunca una respuesta vacía.

| Endpoint | Método | Descripción |
|----------|--------|-------------|
| `/v1/models` | GET | Devuelve el alias `fogar-gateway` y los agentes del roster de OpenClaw que el aparato puede usar (filtrados por `allowed_agents`); sin roster, solo el alias |
| `/v1/chat/completions` | POST | Chat completion; soporta `stream: true/false`. Si el último mensaje `user` falta o solo tiene espacios → `400 empty_message`, sin llamar a OpenClaw. Si el cliente corta antes de la respuesta (con o sin stream), su run (solo ese) se aborta en OpenClaw |

---

## Protocolo WebSocket — resumen rápido

El cliente envía un handshake JSON como primer mensaje. Si es válido, el gateway responde con `ready` y la sesión comienza.

**Cada mensaje y su turno de respuesta (5.0):**
```
→ {"type":"send","text":"Hola"}
← {"type":"message","id":"<runId>","conversation":"<id>","text":"Hola","from":{"source":"device","device_id":"<id>","name":"<nombre>","self":true},"state":"delivered"}
← {"type":"turn_start","turn_id":"t-1","turn_seq":1,"conversation":"<id>","origin":"user","by":{"device_id":"<id>","name":"<nombre>","self":true}}
← {"type":"token","turn_id":"t-1","text":"Hola,"}
← [0xA1][0x00][0x01][PCM16 24kHz...]   ← audio binario con header
← {"type":"turn_end","turn_id":"t-1","reason":"done"}
```

Un `send` con un turno en curso no lo corta: queda `pending` en la cola de OpenClaw (`"interrupt": true` corta y envía; `withdraw` quita un pendiente; `cancel` para el turno para todas las sesiones de la conversación). Hablar encima no corta nada: el barge-in lo decide el cliente. Un aviso (push) se lee en todas las sesiones que tienen abierta su conversación y suena solo en su dispositivo de avisos (el de su dueña si es privada, el del hogar si es común; si no tiene ninguna sesión en ella, le llega igual, en cola detrás de su turno); sin dispositivo de avisos elegido, no suena en ningún sitio. Ver [`docs/client-protocol.md`](docs/client-protocol.md) para la referencia completa incluyendo interrupciones, transcripción, push turns y manejo de errores.

**Dónde suenan los avisos.** Se elige en la web: cada persona, el suyo en «Mis dispositivos» («Avisos en…», uno de sus aparatos personales activos); un dueño o administrador, el del hogar en «Personas» («Avisos del hogar en…», cualquier aparato activo del hogar). Mientras no hay uno elegido, la web avisa «Elige dónde suenan los avisos». Al actualizar a la 5.0 no hay ninguno: hasta elegirlo, los recordatorios solo se leen en las sesiones que tienen abierta su conversación (y siguen en el historial de OpenClaw). También por la API: `PATCH /auth/me` y `PATCH /home`.

---

## Instalación

La vía recomendada es la **instalación nativa** (servicio de usuario de systemd, solo Linux). Docker queda como [alternativa](#docker-alternativa). `<base>` es `~/.fogar-gateway` (o `--home` / `FOGAR_GATEWAY_HOME`).

### Instalación nativa

**Requisitos:** Linux (x86_64 o aarch64) con systemd de usuario y un OpenClaw en este mismo equipo (o accesible por túnel SSH), 2026.6.11 o posterior. Lo demás lo comprueba el instalador. Ejecútalo con tu usuario, no como root: el servicio es de tu usuario.

```bash
curl -fsSL https://github.com/fogarhome/fogar-gateway/releases/latest/download/install.sh | sh
```

Qué hace `install.sh`:

1. Comprueba Linux, la arquitectura y `systemctl --user`. Si falta `git` (`sudo apt-get install -y git`) o linger (`sudo loginctl enable-linger $(id -un)`; sin él, el servicio se para al cerrar tu sesión), enseña el comando y pregunta «¿lo ejecuto yo? [s/N]» (en inglés, «shall I run it? [y/N]»; acepta `s`, `si`, `sí`, `y` o `yes`, sin distinguir mayúsculas): nada con `sudo` sin un sí. Los mensajes salen en español si `LC_ALL`, `LC_MESSAGES` o `LANG` (el primero definido) empieza por `es`, y en inglés en cualquier otro caso. Sin terminal, o con otra respuesta, sale y te dice qué ejecutar. El asistente (`fogar-gw onboard`) y la CLI siguen solo en español.
2. Instala [uv](https://docs.astral.sh/uv/) si falta, y `fogar-gw` con `uv tool install` en la versión de la release, con su `constraints.txt` (las mismas versiones que la imagen Docker). `uv tool update-shell` deja `~/.local/bin` en el PATH de las terminales nuevas; si la tuya aún no lo tiene, el instalador acaba diciéndolo (abre otra terminal o usa `~/.local/bin/fogar-gw`).
3. La primera vez lanza el asistente, `fogar-gw onboard` (abajo). Sin terminal (p. ej. `ssh host 'curl … | sh'`) se para ahí: «Instalado. Configúralo con `fogar-gw onboard` o, si vienes de Docker, `fogar-gw migrate --from-docker DIR` y `fogar-gw install`.»

**Volver a ejecutarlo actualiza:** con el servicio ya instalado, reinstala `fogar-gw`, reinicia el servicio (corta las sesiones abiertas) y pasa `fogar-gw doctor`, sin preguntar nada y sin tocar `config.env`.

**Versión:** el `install.sh` de `releases/latest` instala la última release, y el de una release concreta (`…/releases/download/vX.Y.Z/install.sh`), la suya. Para elegir otra: `curl -fsSL … | FOGAR_VERSION=X.Y.Z sh`. Con `sh -s -- --no-onboard` solo instala `fogar-gw`, sin el asistente (es el primer paso para [pasar de Docker a nativo](#pasar-de-docker-a-nativo)); las versiones anteriores a `fogar-gw onboard` (desde la v1.26.0, la primera con `constraints.txt`) solo se instalan con `--no-onboard` o sin terminal.

#### El asistente: `fogar-gw onboard`

`fogar-gw onboard` pregunta lo mínimo y **verifica cada proveedor contra el servicio real antes de guardarlo** en `config.env`.

En una terminal con color empieza con la línea viva de Fogar escribiendo su nombre (con `FOGAR_REDUCED_MOTION=1`, quieta; en una terminal de menos de 60 columnas, no sale). Cada paso se abre con «● Paso» y lo resuelto queda en «■ …». Los colores dicen siempre lo mismo: naranja es lo que pasa ahora, azul lo resuelto, amarillo ◆ un aviso y rosa ▲ un error; el resto va sin color. Con `NO_COLOR`, `--json` o `--answers`, la salida es texto plano.

1. Si falta `config.env` o la BD, prepara `<base>` como `fogar-gw init` (no toca lo que ya exista).
2. OpenClaw: lo busca en este equipo y, si lo encuentra, lee su token él solo con `openclaw gateway auth-token --show` (busca el binario en el `PATH`, en `~/.npm-global/bin` y en `~/.openclaw/bin`; el token no se muestra). Si no encuentra OpenClaw en este equipo, pregunta host y puerto. Si no puede leer el token o el token no pasa la verificación, lo pide: ejecuta `openclaw gateway auth-token --show` en una terminal de la máquina de OpenClaw y pega el resultado (`openclaw config get gateway.auth.token` lo enseña siempre redactado). Con `--answers` el token va siempre en el fichero. Verifica siempre el token, sea leído o pegado (aunque el gateway ya tenga un token de dispositivo) y dice con qué acceso queda («Conectado a OpenClaw … con permisos de administración»). Solo en loopback: para un OpenClaw de otra máquina, un túnel SSH (`ssh -L 18789:127.0.0.1:18789 …`) o, a mano, `fogar-gw config set OPENCLAW_HOST` y aprobar el gateway en OpenClaw (ver [Conexión con OpenClaw](#conexión-con-openclaw)). Sin OpenClaw verificado no sigue.
3. STT y TTS: servicio propio (`host:puerto` y, si la usa, su clave), ElevenLabs (una sola clave para los dos) o «ahora no» (`off`: sin STT no hay transcripción y sin TTS las respuestas llegan solo en texto).
4. «¿Desde dónde vas a entrar a Fogar?»: una sola elección entre «Desde casa», «Casa y Tailscale», «Casa y mi dominio» y «Casa, Tailscale y mi dominio». El paso acaba diciendo qué ha quedado listo:
   - **Con Tailscale:** si `tailscale serve` ya lleva al gateway, te da su `https://…` y no toca nada. Si no, pregunta antes de activarlo en 443 (o en 8443 si 443 ya sirve otra cosa; si los dos están ocupados, no lo toca). Si Tailscale pide permiso, te enseña `sudo tailscale set --operator=$USER` y lo ejecuta solo si eliges «Ejecutarlo ahora» (y reintenta una vez); si la tailnet no tiene HTTPS, te da el enlace para activarlo. Sin Tailscale te da los comandos oficiales para instalarlo (`curl -fsSL https://tailscale.com/install.sh | sh` y `sudo tailscale up`) y vuelve a mirar cuando quieras, o sigues sin él: nunca lo instala por ti.
   - **Con mi propio dominio:** una URL `https://` (sin ruta, parámetros ni usuario) que tu proxy haga llegar a `http://localhost:8004`; se añade a `PUBLIC_URLS` sin quitar las que ya hay.

   Lo mismo, en cualquier momento: `fogar-gw access setup`.
5. Instala el servicio, o lo reinicia si ya estaba instalado (con terminal pregunta antes, porque corta las sesiones abiertas), y pasa `fogar-gw doctor`. Si la unidad instalada es de otra base (su `FOGAR_GATEWAY_HOME` no es el de esta ejecución; sin él, la de por defecto), se niega antes de preguntar nada: así un `onboard` con otra base no toca el servicio de producción. Con `--no-service` (o `"service": false` en `--answers`) ni instala ni reinicia nada, ni necesita systemd, y los pasos siguientes hablan con el gateway de `--url` (por defecto `http://127.0.0.1:8004`), que arrancas tú. Por ejemplo, un gateway de prueba junto a producción:

   ```bash
   export FOGAR_GATEWAY_HOME=~/.fogar-gateway-prueba
   fogar-gw init
   uvicorn fogar_gateway.main:app --port 8014 &
   fogar-gw --url http://127.0.0.1:8014 onboard --no-service
   ```

   Lo que `onboard` escribe en `config.env` se aplica al reiniciar ese uvicorn (antes de la cuenta y el dashboard, que hablan con él, basta con que esté en marcha). Las vías al dashboard salen con el puerto de `--url`.
6. «Tu cuenta» (4.0): si el hogar aún no tiene dueño, crea la tuya (usuario, nombre y contraseña dos veces). Con ella entras en el dashboard. Con `--answers`, del campo `owner` (`{"username", "display_name", "password"}`); sin él, solo avisa.
7. Termina en el dashboard:
   - las vías para llegar al dashboard desde otro aparato: la recomendada («Tu dashboard») y las demás, cada una con su estado (verificada, sin comprobar…). Por la LAN es `http://<IP de la LAN>:8004/` (o la de `GATEWAY_HOST` si escucha en una dirección concreta; si solo escucha en loopback y no hay otra vía, te dice cómo entrar con un túnel SSH: `ssh -L 8004:127.0.0.1:8004 …` desde tu equipo, que solo escucha en su loopback; si el puerto reenviado escucha en otra dirección (`-g`, `ssh -L 0.0.0.0:…`), todo lo que entra por él es loopback para `/v1`, ver [OpenAI-compatible](#openai-compatible-v1-para-home-assistant)). Para verlas o cambiarlas después: `fogar-gw access` y `fogar-gw access setup`;
   - en una terminal con color, un QR de la vía recomendada para abrirla con el móvil;
   - cómo se entra: con tu usuario y tu contraseña. Por HTTP solo desde este equipo o la tailnet; desde la LAN, por HTTPS (ver [Personas y sesiones](#personas-y-sesiones-auth-40)). El token de administración ya no se imprime ni se copia: es para la CLI y la recuperación, y dice dónde está (`<base>/config.env`, `ADMIN_TOKEN`) y cómo verlo: `fogar-gw token`.

   Después te espera hasta que abres el dashboard y entras («Conectado»); Enter sigue sin esperar. El primer cliente se empareja desde Clientes, en el dashboard. También da la dirección `ws://<host>:8004/ws/stream` para quien configure un cliente a mano.

**El token de administración (`ADMIN_TOKEN`):** desde la 4.0 ya no abre el panel (allí entra cada persona con su cuenta); es la llave de la máquina (`X-Admin-Token`, puede todo), la que usa la CLI y con la que se recupera la cuenta del dueño. El gateway no limita sus intentos: necesita al menos 32 caracteres ASCII imprimibles, sin espacios (`fogar-gw init` genera uno aleatorio de 43; `config set ADMIN_TOKEN` rechaza uno más corto y `doctor` lo marca). `fogar-gw token` lo escribe solo en una terminal; en una tubería, un fichero o con `--json` se niega, para que no acabe en un log. No lo pegues en chats ni en capturas. Sin la CLI, está en `config.env` (`ADMIN_TOKEN`), legible solo por tu usuario.

Si lo cortas a medias, `config.env` queda como estaba más lo ya verificado, y volver a ejecutarlo retoma. También sirve para cambiar de proveedores más adelante (en el menú de `fogar-gw`, «Configurar proveedores»): cada paso que `config.env` ya tiene ofrece primero «Dejar como está», que ni verifica ni escribe nada. Necesita una terminal; para scripts, `fogar-gw onboard --answers respuestas.json` no pregunta nada y un fallo de verificación sale con 1 y su motivo. El fichero lleva secretos: tiene que ser tuyo y con permisos 600. Créalo ya privado, para que no haya un momento en que otros puedan leerlo: `(umask 077; cat > respuestas.json)` y pega el JSON (termina con Ctrl+D), o `install -m 600 /dev/null respuestas.json` antes de editarlo. Escribirlo y después hacer `chmod` deja esa ventana abierta.

```json
{
  "openclaw_host": "127.0.0.1", "openclaw_port": 18789, "openclaw_token": "…",
  "stt": {"provider": "off"},
  "tts": {"provider": "fogar", "url": "localhost:8005", "token": "…"},
  "first_client": {"name": "salón", "type": "ha"},
  "access": {"tailscale_serve": true, "public_urls": ["https://fogar.example.com"]}
}
```

`stt` y `tts`: `{"provider": "fogar", "url"?, "token"?}` (por defecto `localhost:9000` / `localhost:8005`, sin clave), `{"provider": "elevenlabs", "api_key"}` (la misma en los dos; si el TTS de `config.env` ya usa ElevenLabs con otra clave, la nueva del STT se verifica también para el TTS antes de guardarla, salvo con `"tts": {"provider": "off"}`) o `{"provider": "off"}`. `first_client` (opcional): `{"name", "type"}` (`ha`, `esp32`, `web`, `app` o `screen`) crea ese cliente si aún no hay ninguno; sin él o con `null`, no crea ninguno. `access` (opcional): `tailscale_serve` (por defecto `false`) activa `tailscale serve` como arriba pero sin preguntar (si hace falta permiso o HTTPS, solo lo explica y sigue: nunca ejecuta sudo) y `public_urls` (por defecto `[]`) añade esas URLs a `PUBLIC_URLS`; una URL que no valga sale con 2. Sin `access`, el paso no se ejecuta. Con `--answers` no espera al dashboard ni escribe el token. Un campo que sobre, u otro que falte, es un error.

#### Emparejar un cliente

En vez de crear el cliente y copiar su clave a mano, el cliente puede darse de alta solo con un código:

```bash
fogar-gw pair --type screen --name tele --agent j-assistant --allowed-agent j-assistant \
  --set tts_voice=ef_dora --set push_enabled:=false
```

1. Enseña un código de 6 cifras (`482 913`), el nombre del gateway y a qué hora caduca (10 minutos).
2. Escríbelo en la pantalla de emparejamiento del cliente. El cliente encuentra el gateway solo (en el mismo equipo o en la tailnet) y llama a `POST /pair`.
3. `fogar-gw pair` espera hasta que se use: «Cliente «tele» emparejado». Ctrl+C anula el código.

Las opciones son las de `clients add` más `--set` para cualquier otro campo (voz, turno…), y deciden cómo se comporta el cliente. `--scope` (repetible: `conversations:read`, `conversations:write` o `home:read`) fija los permisos que concede el código; sin él, `conversations:write`. Un aparato del hogar nunca llega a `home:admin`. `clients add` acepta el mismo `--scope`. El cliente aporta su tipo, lo que sabe reproducir (`output_mode`) y un nombre propuesto; lo que fijes aquí manda. Con `--no-wait` (o `--json`) enseña el código y termina, para scripts.

El código es de un solo uso. Si alguien escribe 5 códigos erróneos seguidos, se anulan todos los abiertos y hay que abrir otro. `fogar-gw clients add` sigue siendo la vía manual (y la de Home Assistant, que no puede canjear un código): enseña el `token` una sola vez.

#### Sin el instalador

Hace falta además `git`, `curl` y [uv](https://docs.astral.sh/uv/) (`curl -LsSf https://astral.sh/uv/install.sh | sh`). `uv tool` deja `fogar-gw` en `~/.local/bin`: si no está en el PATH, ejecuta `uv tool update-shell` y reabre la terminal, porque `fogar-gw install` pone en la unidad el `fogar-gw` que encuentre en el PATH.

`V` es la última versión de [las releases](https://github.com/fogarhome/fogar-gateway/releases) sin la `v` (p. ej. `V=1.27.0` para la release `v1.27.0`). `constraints.txt` existe desde la v1.26.0 y `fogar-gw migrate`, solo desde la versión que lo incluye.

Cada release publica el wheel `fogar_gateway-<versión>-py3-none-any.whl`, que ya lleva la web compilada (instalar desde `git+` no la trae, porque se compila en la release). Con las mismas versiones que la imagen Docker: cada release publica también `constraints.txt` (exportado de `uv.lock`, sin hashes porque uv no los admite en constraints) y `-c` acepta la URL directamente.

**Con el asistente:**

```bash
V=<versión>   # la última de las releases, sin la v
uv tool install "fogar-gateway @ https://github.com/fogarhome/fogar-gateway/releases/download/v$V/fogar_gateway-$V-py3-none-any.whl" \
  -c https://github.com/fogarhome/fogar-gateway/releases/download/v$V/constraints.txt
loginctl enable-linger $USER        # puede pedir sudo según la distro
fogar-gw onboard
```

**A mano**, paso a paso:

```bash
V=<versión>   # la última de las releases, sin la v
uv tool install "fogar-gateway @ https://github.com/fogarhome/fogar-gateway/releases/download/v$V/fogar_gateway-$V-py3-none-any.whl" \
  -c https://github.com/fogarhome/fogar-gateway/releases/download/v$V/constraints.txt
fogar-gw init               # <base>, config.env con ADMIN_TOKEN aleatorio y la BD
fogar-gw config set OPENCLAW_TOKEN   # prompt oculto; ver la nota de debajo
fogar-gw config set OPENCLAW_HOST <host>   # solo si OpenClaw no está en esta máquina (y OPENCLAW_PORT, por defecto 18789)
loginctl enable-linger $USER        # puede pedir sudo según la distro
fogar-gw install            # crea el servicio de usuario y lo arranca
fogar-gw doctor             # configuración, BD, permisos, servicio y gateway
fogar-gw logs -n 20         # últimas líneas del journal (-f para seguir)
```

- **`OPENCLAW_TOKEN`** es el token del gateway de OpenClaw (el de su configuración). El gateway no lo exige: `install` y `serve` arrancan igual y solo registran un WARNING («OPENCLAW_TOKEN está vacío…»), y `config validate` y `doctor` lo marcan como aviso. Pero sin un token válido OpenClaw rechazará la conexión: `/ready` da 503 y las sesiones se cierran, porque el orquestador es la única dependencia fatal. Los secretos se piden por prompt oculto, ver `config set` en [Admin](#admin-requiere-x-admin-token). `OPENCLAW_HOST` y `OPENCLAW_PORT` se cambian con `fogar-gw config set <AJUSTE> <valor>` (solo los secretos se piden por prompt; sin valor, un ajuste normal sale con 2).
- **Conexión con OpenClaw:** ver [abajo](#conexión-con-openclaw).
- **Linger y sesión:** `systemctl --user` necesita una sesión con bus de usuario: SSH normal sí; con `su` o `sudo -iu`, no (define antes `XDG_RUNTIME_DIR=/run/user/$(id -u)`).

#### Conexión con OpenClaw

El gateway se conecta a OpenClaw como `gateway-client`/`backend` pidiendo `operator.read`, `operator.write` y `operator.admin` (ninguna función usa todavía el admin), y firma cada conexión con su **identidad de dispositivo**: una clave Ed25519 propia de la instalación, en `<base>/data/openclaw-device.json` (permisos 600; es una credencial, como `config.env`).

- **OpenClaw en esta máquina:** basta el token compartido (`OPENCLAW_TOKEN`); OpenClaw se fía de la conexión local y concede los permisos pedidos, sin emparejar.
- **OpenClaw en otra máquina** (Tailscale, LAN, o en Docker con el puerto publicado): la primera vez OpenClaw pide aprobar el gateway como dispositivo. El gateway lo dice en el log, en `fogar-gw status`/`doctor` y en `/system/state`, con el comando exacto: `openclaw devices approve <id>`, en la máquina de OpenClaw. Tras aprobarlo, el gateway recibe un **token de dispositivo**, lo guarda en ese fichero y conecta con él (aunque cambie el token compartido); si OpenClaw lo revoca, vuelve al token compartido y, si hace falta, a pedir aprobación.
- **Por Tailscale (lo recomendado para otra máquina):** si OpenClaw se publica en la tailnet con su `tailscale serve` (`gateway.tailscale.mode: serve`), sigue escuchando solo en loopback y se llega con TLS: `OPENCLAW_HOST=<máquina>.<tailnet>.ts.net`, `OPENCLAW_PORT=443` y `OPENCLAW_TLS=true`. No abras el puerto 18789 a la red ni lo pongas detrás de otro proxy inverso: con cabeceras de proxy y sin `gateway.trustedProxies`, OpenClaw responde 403.

> **El gateway nunca crea una BD por su cuenta.** Si no la encuentra en `<base>/data/gateway.db`
> (o en la `DATABASE_URL` configurada), se para y explica qué hacer: así no arranca vacío, sin
> clientes, por haberse equivocado de directorio. La crea `fogar-gw init` (o `fogar-gw migrate`,
> que la copia de Docker). La imagen Docker arranca con `fogar-gw serve --create-db`, así que
> `docker compose up` con un `./data` vacío funciona como siempre.

### Servicio (systemd de usuario)

Solo en Linux con systemd. `fogar-gw install` crea `~/.config/systemd/user/fogar-gateway.service` y lo arranca. Exige una config válida, la BD ya creada (`fogar-gw init` o `fogar-gw migrate`; `install` nunca la crea) y el puerto 8004 libre (para el contenedor antes). Si el servicio ya es el nuestro y está activo, reinstala, y si la unidad cambia, lo reinicia. `uninstall` y sobrescribir una unidad distinta con `install` piden confirmación (para scripts, `fogar-gw --yes …`). `install`, `start`, `stop` y `restart` solo actúan sobre la unidad de esta base: si su `FOGAR_GATEWAY_HOME` es otro (sin él, la base por defecto, `~/.fogar-gateway`), se niegan sin tocarla y dicen cuál es su base; para manejarla, `fogar-gw --home <su base> …`. Así un `restart` (o el reinicio que ofrece `config set`) con `FOGAR_GATEWAY_HOME` de prueba no reinicia producción.

- **Ejecutable:** usa el `fogar-gw` del PATH. Rechaza uno dentro de un clon o de una instalación editable, porque la unidad se rompería al moverlo: instala con `uv tool install` o pasa `--exe RUTA` (una ruta relativa se guarda absoluta, sin resolver el enlace).
- **Linger:** sin `loginctl enable-linger $USER` el servicio se para al cerrar tu sesión; `install` avisa si falta.
- **Registros:** `fogar-gw logs` lee el journal. `--level` y `--session` filtran por nivel mínimo y por sesión o cliente, tanto con `LOG_FORMAT=json` como con texto.

### Actualizar

Las migraciones de la BD se aplican solas al arrancar y no hay vuelta atrás automática de versión: copia `<base>/data/gateway.db` antes. Después, vuelve a ejecutar el instalador: instala la última release encima, reinicia el servicio y pasa `fogar-gw doctor`, sin preguntar (`FOGAR_VERSION=X.Y.Z` para otra versión).

```bash
curl -fsSL https://github.com/fogarhome/fogar-gateway/releases/latest/download/install.sh | sh
```

**De la 3.x a la 4.0: instalación nueva.** La 4.0 empieza un esquema de base de datos nuevo (hogar, personas y dispositivos) y no convierte el de la 3.x: con una BD 3.x el gateway no arranca y lo dice. Copia la carpeta base por si acaso, desinstala (`fogar-gw uninstall --purge`), instala la 4.0 con el comando de arriba y pasa `fogar-gw onboard`, que crea la cuenta del dueño. Después, vuelve a emparejar cada aparato (los códigos de `fogar-gw pair`) y crea las cuentas del resto de la casa (`fogar-gw users add` o la web). Las conversaciones siguen en OpenClaw; el gateway empieza su índice de cero. La web ya no entra con el token: entra con tu cuenta, y por HTTP solo desde la misma máquina o la tailnet.

**Al actualizar a 5.0:** los clientes de la 4.x (la tele, la voz de Mac y Android, la app de iPhone) dejan de conectar (cierre 4003) hasta que tengan su versión 5.0: actualiza primero los clientes, o tenlos listos. No hay que volver a emparejarlos: su token sigue valiendo.

`/v1` exige siempre Bearer: Home Assistant (o lo que entrara por loopback o por `TRUSTED_NETWORKS` sin clave) recibe 401 hasta que le das la clave de un aparato. Si ya tenía una de la 4.x, sigue valiendo; si no, créala con `fogar-gw clients add --name "Home Assistant" --type ha` y pega el `token` como API key en la integración. `TRUST_LOOPBACK`, `TRUSTED_NETWORKS`, `SHUTDOWN_DRAIN_S` y `ORCHESTRATOR_RECONNECT_MAX_DURATION` ya no se usan: si siguen en `config.env`, el gateway arranca igual, los ignora y lo avisa una vez en el log; `fogar-gw doctor` y `config validate` los nombran como aviso y `fogar-gw config unset TRUSTED_NETWORKS` (y los demás) los quita. `fogar-gw migrate --from-docker` no los copia.

**Al actualizar a 3.0:** una entrada inválida en `TRUSTED_PROXIES` (también un `/0`, que antes se aceptaba) ya no se ignora: el gateway falla cerrado y Home Assistant sin Bearer recibe 401 en `/v1`. La CLI 2.x aún no lo comprueba: antes de actualizar, revisa `TRUSTED_PROXIES` y `TRUSTED_NETWORKS` a mano en `<base>/config.env` (solo IPs o CIDRs separados por comas, sin `/0` ni `%`). Después, `fogar-gw doctor` (en una actualización nativa, el instalador lo pasa al terminar sobre la base del servicio) o `fogar-gw config validate` te dicen si queda alguna entrada mal. `ADMIN_TOKEN` necesita al menos 32 caracteres: uno más corto sigue funcionando (el gateway arranca y avisa en el log), pero `fogar-gw doctor` y `config validate` lo marcan como fallo y `config set ADMIN_TOKEN` ya no acepta uno así; cámbialo con `fogar-gw config set ADMIN_TOKEN`: `init` genera uno de 43, pero si ya tienes `config.env` no lo toca (y `config unset ADMIN_TOKEN` no se acepta). Cuentan solo caracteres ASCII imprimibles sin espacios. Los plazos y backoffs (`HANDSHAKE_TIMEOUT_S`, `TURN_TIMEOUT_S`, `IDLE_TIMEOUT_S`, `PUSH_TTS_DRAIN_TIMEOUT_S`, `TTS_AUTH_TIMEOUT_S`, `TRANSCRIBER_READY_TIMEOUT_S`, `TRANSCRIBER_RECONNECT_MAX_DURATION` y los `*_RECONNECT_*_BACKOFF`) deben ser mayores que 0, y ningún número decimal admite `inf` ni `nan`: con uno así el gateway ya no arranca (antes arrancaba inservible). Si el servicio no arranca tras actualizar, el instalador lo dice, pasa igualmente `fogar-gw doctor` y termina con error. `config validate` lo marca y se corrige con `fogar-gw config set <AJUSTE> <valor>` o `fogar-gw config unset <AJUSTE>` (vuelve al valor por defecto), que funcionan aunque la configuración no valide: solo rechazan un valor que deje inválido el propio ajuste (y `config unset ADMIN_TOKEN`). Si tras el cambio queda algún fallo, en ese u otro ajuste, lo avisan sin deshacer nada; `config validate` y `doctor` dicen si la configuración es válida. Además, un `X-Real-IP` de loopback ya solo cuenta si el peer del proxy también es loopback: un proxy en este host que llega por la IP de LAN o por el bridge de Docker (p. ej. 172.17.0.1) con `X-Real-IP: 127.0.0.1` ya no queda exento y debe usar Bearer.

A mano: instala la nueva versión sobre la anterior y reinicia (pregunta; en scripts, `fogar-gw --yes restart`).

```bash
V=<versión>   # la nueva, sin la v
uv tool install --reinstall "fogar-gateway @ https://github.com/fogarhome/fogar-gateway/releases/download/v$V/fogar_gateway-$V-py3-none-any.whl" \
  -c https://github.com/fogarhome/fogar-gateway/releases/download/v$V/constraints.txt
fogar-gw restart
fogar-gw version           # versión de la CLI y del gateway en marcha
```

### Desinstalar

```bash
fogar-gw uninstall                # quita el servicio (pregunta); no toca los datos
uv tool uninstall fogar-gateway   # quita el programa
```

Los datos (`config.env`, `data/gateway.db`) quedan en `<base>`.

Para borrarlo todo de una vez (por ejemplo, para empezar de cero con `install.sh`):

```bash
fogar-gw uninstall --purge        # servicio, <base> entero (con la BD) y el programa
```

Pide escribir la ruta de `<base>` para confirmar (`--yes` lo salta en scripts) y no tiene vuelta atrás: se pierden los clientes y sus claves, `config.env` con sus tokens y la identidad del gateway ante OpenClaw, así que habrá que emparejar los clientes otra vez. Solo borra dentro de `<base>`: se niega si no parece una instalación (sin `ADMIN_TOKEN` en `config.env` ni `data/gateway.db`), si la BD de `config.env` (`DATABASE_URL`) vive fuera o `data/` es un enlace a otro sitio, y con un gateway en marcha fuera del servicio. El servicio y el programa solo los quita si son de esa base (sin servicio instalado, solo con la base por defecto, `~/.fogar-gateway`). Si el programa no se instaló con `uv tool`, te dice cómo quitarlo. No toca uv, linger, Tailscale ni OpenClaw. En Docker: `docker compose down -v`.

### Pasar de Docker a nativo

`fogar-gw migrate --from-docker <carpeta de compose>` copia `.env` y `data/gateway.db` de una instalación Docker a `<base>` (y `data/openclaw-device.json`, la identidad ante OpenClaw, si existe: así sigue emparejado). Solo vale para una instalación Docker de la v2.0.0 o posterior: las versiones 1.x no se migran, se instala desde cero. Procedimiento, con la ventana de parada y la vuelta atrás:

1. **Misma versión a los dos lados.** Despliega la versión vN en Docker e instala la vN nativa **sin arrancarla**, con el instalador y `--no-onboard`:
   ```bash
   curl -fsSL https://github.com/fogarhome/fogar-gateway/releases/latest/download/install.sh | FOGAR_VERSION=<N> sh -s -- --no-onboard
   ```
   y **nada más** (ni `fogar-gw init`, ni `fogar-gw onboard`, ni `fogar-gw install`: `migrate` se niega si `<base>` ya tiene `config.env`, BD o restos de SQLite). El instalador acaba con «Configúralo con `fogar-gw onboard` o, si vienes de Docker, …»: tú sigues la vía de Docker, con `migrate` en el paso 3 e `install` en el 5. `<N>` es la versión de Docker sin la `v`: `fogar-gw version` da la nativa y `docker exec fogar_gateway fogar-gw version` la de Docker. El instalador ya deja linger activado.
2. **Ventana sin sesiones.** `docker exec fogar_gateway fogar-gw status` debe mostrar 0 sesiones.
3. **Parar, copiar y migrar.** `docker compose stop` (sin `down`), copia de seguridad de la BD y migra (usa un usuario que pueda leer `<carpeta>/data` y su `gateway.lock`):
   ```bash
   cp -a <carpeta de compose>/data/gateway.db* <destino de la copia>/
   fogar-gw migrate --from-docker <carpeta de compose>
   ```
4. **Revisar la config migrada, antes de instalar.** `fogar-gw config show` y `fogar-gw doctor`: los nombres que solo resuelven en la red de Docker (nombres de servicios de compose, `host.docker.internal`) hay que cambiarlos con `fogar-gw config set <AJUSTE> <valor>` (p. ej. `fogar-gw config set OPENCLAW_HOST <host>`).
5. **Instalar el servicio:** `fogar-gw install`, que comprueba que el 8004 está libre.
   Si `GATEWAY_HOST` es una IP concreta de la LAN, `install` espera a `/healthz` en la URL de `--url` (por defecto `http://127.0.0.1:8004`) y daría error por tiempo: usa `fogar-gw --url http://<IP>:8004 install`. La comprobación de puerto libre sí mira solo `127.0.0.1`.
6. **Verificar.** `curl http://127.0.0.1:8004/ready`, una sesión de voz o texto, `/v1` con y sin clave, los mismos clientes (`fogar-gw clients list`), `fogar-gw doctor` y `fogar-gw logs -n 20`.
7. **Vuelta atrás.** `fogar-gw stop` y `systemctl --user disable fogar-gateway.service` (si no, tras un reinicio de la máquina arrancan los dos y se pelean por el 8004), y después `docker compose up -d`. Para volver a nativo: `docker compose stop` y `fogar-gw install` (con la unidad igual no la toca: la habilita y la arranca). Los cambios de clientes hechos en un lado no están en la BD del otro.
8. **Retirar el contenedor**, más tarde: `docker compose down`, sin borrar la imagen ni la carpeta.

Qué hace `migrate` (`src/fogar_gateway/cli/migrate.py`):

- Copia la BD con la API de backup de sqlite3, comprueba `integrity_check` y que los clientes coinciden; **nunca escribe el origen** (lo abre en solo lectura). Si algo falla, no queda nada a medias, salvo un corte brusco (SIGKILL, corte de luz) entre la copia de la BD y la de `config.env`: entonces borra a mano lo creado en `<base>` y repite.
- Exige la misma revisión de Alembic a los dos lados; si no, se niega y dice qué desplegar.
- Se niega si el contenedor sigue en marcha mientras el gateway de Docker tenga el candado sobre `data/gateway.lock` (por defecto sí; no con `SINGLE_PROCESS_LOCK=false`), si `<base>` ya tiene `config.env` o BD (o restos `-journal`/`-wal`/`-shm`), o si el `DATABASE_URL` del `.env` no apunta a `/app/data/gateway.db`.
- Del `.env` quita `DATABASE_URL`, `GATEWAY_UID`, `GATEWAY_GID` y `TZ` (la nativa usa la zona del host), y añade `LOG_FORMAT=json` si falta (la imagen lo fijaba). Una clave del `.env` que no sea un ajuste del gateway hace que se niegue ("La configuración migrada no es válida"): quítala antes y repite.
- Crea `<base>` y `data/` con permisos 700 y `config.env` y la BD con 600. No migra los datos más allá de `data/gateway.db`: si montabas más cosas en el volumen, cópialas a mano.

### Desarrollo en la misma máquina que producción

Usa siempre `./.dev` como base (`--home ./.dev` o `FOGAR_GATEWAY_HOME=./.dev`): sin él, la CLI y el gateway comparten `~/.fogar-gateway`, o sea, la BD de producción. Además, `serve` usa siempre el puerto 8004 y la CLI apunta por defecto a `http://127.0.0.1:8004`: en la misma máquina el gateway de desarrollo choca con el de producción, o `clients list` acaba hablando con producción. Por eso el ejemplo de abajo usa el puerto 8014 (el 8005 es el `TTS_WS_URL` por defecto de fogar-speaker) con `uvicorn` y apunta la CLI con `FOGAR_GW_URL` (o `--url`).

```bash
git clone https://github.com/fogarhome/fogar-gateway && cd fogar-gateway
uv sync --extra dev   # crea .venv desde uv.lock (o `pip install -e .[dev]`, sin versiones fijadas)
source .venv/bin/activate   # sin esto (o `uv run fogar-gw …`), `fogar-gw` es el de producción
export FOGAR_GATEWAY_HOME=./.dev FOGAR_GW_URL=http://127.0.0.1:8014
fogar-gw init                # una vez
uvicorn fogar_gateway.main:app --port 8014 --reload   # `fogar-gw serve` fija el 8004
fogar-gw clients list        # habla con el 8014
```

### Docker (alternativa)

```bash
docker compose up
```

> El contenedor corre como no-root. Antes del primer `docker compose up`
> define `GATEWAY_UID`/`GATEWAY_GID` en `.env` con el dueño de `./data`
> (`echo "GATEWAY_UID=$(id -u)" >> .env && echo "GATEWAY_GID=$(id -g)" >> .env`).
> Si `./data` viene de un despliegue anterior como root: `sudo chown -R "$(id -u):$(id -g)" data`.
> El contenedor no ve la zona horaria del host: la fija `TZ` (por defecto `Europe/Madrid`; otra con
> `TZ=America/Bogota` en el `.env`). De ella depende el corte del día de las conversaciones
> (`CONVERSATION_DAY_START_HOUR`, hora local); sin `TZ` sería UTC.
> Detalles en `docker-compose.yml`. Para pasar a nativo, ver [Pasar de Docker a nativo](#pasar-de-docker-a-nativo).

---

## Configuración

Variables de entorno o, si no, de `<base>/config.env` (instalación nativa). En Docker van en el `.env` junto a `docker-compose.yml`, que las pasa como variables. Formato `host:port` sin protocolo para todos los URLs. Esta tabla es un resumen: la referencia completa, con todos los plazos y sus defaults, es [`.env.sample`](.env.sample).

| Variable | Default | Descripción |
|----------|---------|-------------|
| `DATABASE_URL` | `sqlite:///data/gateway.db` | Base de datos SQLite local (identidad y configuración de clientes). Una ruta relativa cuelga de `<base>` |
| `GATEWAY_HOST` | `0.0.0.0` | Host de escucha de `fogar-gw serve` (`127.0.0.1` detrás de un proxy en el mismo host) |
| `GATEWAY_NAME` | — | Nombre con el que se presenta a los clientes al emparejarse (`gateway.name` de `POST /pair`; no se publica en `/.well-known/fogar-gateway`). Vacío = nombre del equipo; en Docker conviene fijarlo |
| `PUBLIC_URLS` | — | URLs `https://` propias por las que se llega al gateway (CSV, sin ruta, parámetros ni usuario), p. ej. `https://fogar.example.com`. Vacío = ninguna |
| `TRANSCRIBER_WS_URL` | `localhost:9000` | fogar-transcriber |
| `TTS_WS_URL` | `localhost:8005` | fogar-speaker |
| `TTS_TOKEN` | — | Token de auth para fogar-speaker; vacío si el TTS no exige clave |
| `OPENCLAW_HOST` | `127.0.0.1` | Host de OpenClaw |
| `OPENCLAW_PORT` | `18789` | Puerto de OpenClaw |
| `OPENCLAW_TLS` | `false` | `true` = conecta por `wss://` (OpenClaw de otra máquina tras TLS, p. ej. Tailscale) |
| `OPENCLAW_TOKEN` | — | Token de OpenClaw. Sin él el gateway arranca con un aviso, pero OpenClaw rechaza la conexión (`/ready` 503) |
| `ADMIN_TOKEN` | — | Token de la máquina (`X-Admin-Token`, puede todo: la CLI y la recuperación); la web ya no lo usa, entra con usuario y contraseña. Vacío lo deshabilita. Al menos 32 caracteres (uno más corto arranca con un aviso, pero `doctor` lo marca como fallo) |
| `DEFAULT_AGENT` | — | Agente por defecto de la instalación (#245): en la cascada, después del `default_agent` del cliente y antes del de OpenClaw; también el de una conversación nueva. Vacío = el de OpenClaw. Si no está en OpenClaw, avisa al conectar. Es un id de agente de OpenClaw (letras, cifras, `_` y `-`, hasta 64, empezando por letra o cifra), la misma regla que `default_agent` y `allowed_agents` de un cliente. Sin distinguir mayúsculas: se usa en minúsculas, como en OpenClaw (`Main` es `main`); `/home/devices` guarda y devuelve también en minúsculas `default_agent` y `allowed_agents` |
| `CONVERSATION_DAY_START_HOUR` | `5` | Hora local (0–23) a la que empieza el día de las conversaciones por defecto. En Docker la hora local es la de `TZ` (ver [Docker](#docker-alternativa)) |
| `ORCHESTRATOR_RECONNECT_INITIAL_BACKOFF` | `1.0` | Backoff inicial en reconexión del orquestador (s) |
| `ORCHESTRATOR_RECONNECT_MAX_BACKOFF` | `60.0` | Backoff máximo en reconexión del orquestador (s) (el gateway reintenta sin parar, sin tope de duración) |
| `SINGLE_PROCESS_LOCK` | `true` | `flock` sobre `gateway.lock` junto a la BD: un segundo proceso (p. ej. `--workers 2`) no arranca. Multi-worker no está soportado |
| `SESSION_REVALIDATE_INTERVAL_S` | `10.0` | Cada sesión WS se revalida contra la BD; cliente borrado/desactivado/clave rotada → cierre 4200 (`credential revoked`); agente de la sesión quitado de su `allowed_agents` → cierre 4020 (`agent not permitted`, la clave sigue valiendo). Ver [códigos de cierre](docs/client-protocol.md#códigos-de-cierre) |
| `TRANSCRIBER_RECONNECT_INITIAL_BACKOFF` | `1.0` | Backoff inicial en reconexión del transcriptor (s) |
| `TRANSCRIBER_RECONNECT_MAX_BACKOFF` | `60.0` | Backoff máximo en reconexión del transcriptor (s) |
| `TRANSCRIBER_RECONNECT_MAX_DURATION` | `300.0` | Tiempo hasta DEGRADED del transcriptor (s) |
| `TTS_RECONNECT_INITIAL_BACKOFF` | `1.0` | Backoff inicial del breaker de TTS (s) |
| `TTS_RECONNECT_MAX_BACKOFF` | `60.0` | Backoff máximo del breaker de TTS (s) — sin estado DEGRADED, cada turno elegible reintenta |

### Proveedores de voz (STT/TTS)

El STT y el TTS pasan por una capa de proveedores (`src/fogar_gateway/services/speech/`). Los clientes no notan la diferencia: mismos formatos de audio y mismos mensajes.

| Variable | Default | Descripción |
|----------|---------|-------------|
| `STT_PROVIDER` | `fogar` | `fogar` (fogar-transcriber), `elevenlabs` (Scribe v2 Realtime) u `off` (sin transcripción: el audio de los clientes se descarta y solo entra texto) |
| `TTS_PROVIDER` | `fogar` | `fogar` (fogar-speaker), `elevenlabs` (stream-input, `eleven_flash_v2_5`) u `off` (sin respuestas en audio: solo en texto) |
| `ELEVENLABS_API_KEY` | — | Obligatoria si algún proveedor es `elevenlabs` (permisos Text to Speech y Speech to Text) |
| `ELEVENLABS_TTS_DEFAULT_VOICE` | `gc61tTk93h3LBXPxew2V` | Voz por defecto; las voces de clientes que no son de ElevenLabs (p. ej. `af_heart`) caen en ella o se traducen con `TTS_VOICE_MAP` |
| `STT_AUDIO_GATE` | `energy` | Puerta de energía del STT de ElevenLabs (solo se envía audio mientras se habla); `off` la quita. fogar no la usa |
| `STT_GATE_RMS_DBFS` | `-45` | Umbral de la puerta de energía |

`ELEVENLABS_BASE_URL` y `ELEVENLABS_TTS_MODEL` están en `.env.sample`; el resto de ajustes de ElevenLabs son constantes del adaptador (`src/fogar_gateway/services/speech/elevenlabs/`). El proveedor se elige con `STT_PROVIDER`/`TTS_PROVIDER` al arrancar; cambiarlo exige reiniciar. Con el plan Creator, ElevenLabs conserva el audio y las transcripciones (la retención cero es solo enterprise).

`silence_timeout_s` (umbral del aviso de silencio, que nunca cierra la sesión) es un campo de configuración **por cliente** en la base de datos, no una variable de entorno global — ver `ClientRecord` en `src/fogar_gateway/db/models.py`.

---

## Desarrollo y tests

### Dependencias

`pyproject.toml` declara los rangos y **`uv.lock` fija las versiones exactas**, con hashes: el CI (`uv sync --locked`), la imagen Docker (`uv export` + `pip install --require-hashes`) y Dependabot (ecosistema `uv`) parten de él. Al cambiar una dependencia en `pyproject.toml`, ejecuta `uv lock` y commitea los dos ficheros; el CI falla si `uv.lock` no corresponde a `pyproject.toml`.

```bash
uv sync --extra dev    # entorno de desarrollo desde uv.lock
uv lock --upgrade      # sube todo dentro de los rangos (Dependabot lo hace por paquete)
```

### Tests

```bash
# Tests (pytest.ini excluye por defecto los marcadores e2e_real y live)
PYTHONPATH=. pytest

# Test específico
PYTHONPATH=. pytest tests/integration/test_rest_health.py

# Lint
ruff check src/ tests/
```

---

## Smoke-test E2E real (contra OpenClaw de producción)

`tests/e2e/` valida el pipeline completo contra el OpenClaw **real** de este mismo
servidor: turno de texto, cancelación, sesiones concurrentes y uso de tools. Nunca
corre automáticamente — ni en la CI normal, ni con un `pytest` a secas (marker
`e2e_real`, deseleccionado por defecto en `pytest.ini`).

Solo se dispara manualmente vía el workflow `E2E Real Smoke Test`
(`workflow_dispatch`), gated por un GitHub Environment con aprobación requerida.
Antes de poder usarlo hace falta configurar, una sola vez, lo siguiente
(fuera de este repo):

1. **Agente de test dedicado en OpenClaw** — con personalidad/memoria aislada de
   los agentes de producción, y alguna tool/skill simple y determinista habilitada
   (para `test_tool_use.py`). Nunca se testea contra un agente real.
2. **Runner self-hosted dedicado**, separado del runner `green-house` existente
   (que sirve a otro repo). Registrar una segunda instancia de
   [`actions-runner`](https://github.com/actions/runner) en este servidor, con
   label `fogar-e2e`, contra este repo.
3. **GitHub Environment `e2e-real-production`** (Settings → Environments):
   - *Required reviewers*: el usuario propietario del repo, explícitamente.
   - *Deployment branches*: restringido a `main`.
   - *Environment variable* `E2E_TEST_AGENT`: nombre del agente de test del punto 1.

Ejecución manual local (sin pasar por GitHub Actions), útil para validar cambios
en la suite antes de fiarse del workflow:

```bash
set -a; . ./.env; set +a      # o el fichero donde tengas ADMIN_TOKEN y OPENCLAW_TOKEN
PYTHONPATH=. E2E_TEST_AGENT=<agente-de-test> pytest tests/e2e -m e2e_real -v
```

`OPENCLAW_TOKEN` y `ADMIN_TOKEN` nunca se guardan en GitHub: el proceso de test los
lee de sus variables de entorno. La suite no lee ningún `.env` ni `config.env` por su
cuenta (`tests/conftest.py` fija una base temporal), por eso hay que exportarlos.

**Nota para el workflow de GitHub Actions:** el checkout que hace `actions/checkout`
en el runner es limpio y **no incluye `.env`** (está en `.gitignore`). Por eso, para
la ruta de CI, `ADMIN_TOKEN` y `OPENCLAW_TOKEN` deben estar exportados como
variables de entorno reales en el propio proceso del runner self-hosted (p. ej.
vía `Environment=` en su unidad systemd) — no en un `.env` dentro del checkout ni
en GitHub Secrets. El job incluye un paso de verificación previo al de tests que
falla explícitamente (`exit 1`) si alguna de las dos falta, para que un runner mal
configurado rompa de forma visible en lugar de pasar en silencio sin cobertura
real.

---

## Arquitectura interna

El gateway instancia un `FogarBridge` por cada sesión WebSocket. Los tres servicios downstream (OpenClaw, Transcriber, TTS) tienen reconexión automática con backoff exponencial, unificada en `services/reconnection.py` (estados compartidos CONNECTED / RECONNECTING / DEGRADED). OpenClaw y los proveedores de voz son singletons de proceso; con el proveedor `fogar`, cada sesión de audio abre su propio `ReconnectingTranscriberClient` y cada turno su propio `TTSClient`.

```
src/fogar_gateway/
├── paths.py                 directorio base, config.env y ruta de la BD (compartido con la CLI)
├── access.py                vías para llegar al gateway: LAN, Tailscale, PUBLIC_URLS; verificación y recomendada
├── alembic.ini, migrations/ migraciones de Alembic, dentro del paquete instalado
├── web/                     el panel compilado (lo mete el build en el wheel; no está en git)
├── api/
│   ├── routes.py            WebSocket /ws/stream — handshake, ready, ciclo de vida del bridge
│   ├── pairing_routes.py    /.well-known/fogar-gateway, POST /pair y /home/pairing
│   ├── home_routes.py       /home — el hogar, sus personas y sus aparatos
│   ├── system_routes.py     /system — sesiones, servicios, voz, vías de acceso y estado en vivo (SSE)
│   ├── conversation_routes.py /conversations — lista, mensajes (chat.history) y alta del panel
│   ├── openai_routes.py     /v1/models, /v1/chat/completions
│   ├── web_routes.py        el panel en / (caché, CSP y vuelta a index.html)
│   ├── health_routes.py     /healthz, /ready
│   └── deps.py              quién llama y require_scope (permisos por aparato)
├── core/
│   ├── config.py            Settings (pydantic-settings, <base>/config.env)
│   ├── agent_policy.py      resolve_agent() — cascada y política de agentes
│   ├── network.py           resolve_client_ip() — la IP real tras un proxy, para los logs
│   ├── startup_checks.py    lock de proceso único y comprobaciones de arranque
│   ├── logging.py           formatos de log y fingerprint_key()
│   └── session_key.py       make_session_key() — la clave de antes de M19 (`agent:{agente}:{client.id}`)
├── db/
│   ├── models.py            ClientRecord (identidad y config por cliente), ConversationRecord (índice de conversaciones)
│   └── database.py          get_engine() / run_migrations() / get_db_session()
├── cli/                     fogar-gw — init, serve, menú, onboard (el asistente; verify.py comprueba cada
│                            proveedor contra el servicio real), access, token, pair, clients, doctor, config,
│                            status, version, servicio systemd (install/start/stop/restart), logs y migrate
└── services/
    ├── bridge.py            FogarBridge — coordinador de sesión WS
    ├── channel.py           ConversationChannel / ChannelHub — el canal de cada conversación: dueño de sus
    │                        runs en OpenClaw, reparte cada turno a todas las sesiones adjuntas
    ├── turns.py             TurnController — la parte de una sesión en el canal: sus pares de turno, su TTS
    ├── conversations.py     conversaciones: reglas del día, tabla, índice
    ├── pairing.py           PairingStore — códigos de emparejamiento en memoria
    ├── db_client.py         DbClient — lee ClientRecord de SQLite local (sin caché)
    ├── outbound.py          QueuedSender — salida serializada hacia cada cliente
    ├── pacing.py            AudioPacer — el audio TTS a 1× con un pequeño adelanto
    ├── reconnection.py      ConnectionState / ServiceStatus / to_wire_state() — compartido por los 3 wrappers
    ├── teardown.py          Teardown — cierre ordenado sin perder errores ni cancelaciones
    ├── transcriber_client.py       WebSocket → fogar-transcriber, protocolo puro (sin reconexión)
    ├── transcriber_reconnecting.py ReconnectingTranscriberClient — uno por sesión de audio, backoff en background
    ├── tts_client.py                WebSocket → fogar-speaker, protocolo puro, creado por turno
    ├── tts_reconnecting.py          ReconnectingTTSClient — singleton, backoff perezoso por turno
    ├── orchestration.py     OrchestratorError (códigos estables)
    ├── pipeline_tracker.py  PipelineTracker — latencias por turno
    ├── session_registry.py  SessionRegistry — observabilidad de sesiones
    ├── speech/              Proveedores STT/TTS: ports.py, factory.py, fogar.py, elevenlabs/, off.py (voz desactivada)
    └── openclaw/
        ├── client.py        OpenClawClient — WebSocket v4, multiplexado; auth y emparejamiento
        ├── device.py        identidad de dispositivo (Ed25519, firma v3) y token de dispositivo
        ├── reconnecting.py  ReconnectingOpenClawClient — backoff + DEGRADED
        ├── dispatcher.py    FrameDispatcher — enruta frames a turn/client registry
        ├── registry.py      TurnRegistry + ClientRegistry (sesiones vivas por cliente: revocación, la sesión del dispositivo de avisos, broadcast_status)
        └── models.py        GatewayInfo, OrchestratorAccess (permisos y emparejamiento)
```

En la raíz del repo, `install.sh` es el instalador nativo (se publica como asset de cada release; el job `installer-smoke` del CI lo prueba).

## Licencia

MIT. Ver [LICENSE](LICENSE).
