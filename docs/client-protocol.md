# Protocolo de cliente — fogar-gateway

Guía completa para implementar clientes que se conecten a fogar-gateway. Cubre el ciclo de vida de la sesión WebSocket, todos los mensajes posibles en ambas direcciones y el formato de audio binario.

> **¿Integras Home Assistant u otro cliente OpenAI-compatible?** Eso va por REST (`/v1/*`), no por este protocolo WebSocket. Su modelo de autenticación (origen de confianza o `Authorization: Bearer <client_key>`) está en el [README](../README.md#openai-compatible-v1-para-home-assistant).

---

## Versión del protocolo y extensión

Este documento describe la **versión 5** del protocolo. El cliente la declara en el handshake (`protocol`, §1) y el gateway la confirma en `ready` (`protocol`, §2). Un handshake sin `protocol`, o con otra versión, se cierra con **4003** antes de validar el resto del handshake y la credencial (solo ganan los cierres previos: 4001 frame binario, 4002 demasiado grande, 4900 timeout, 4000 JSON que no es un objeto); el 4003 no se reintenta solo: hay que actualizar el cliente.

Reglas para que el protocolo crezca sin romper a nadie:

- Un cliente **ignora** los campos y los tipos de mensaje que no conoce.
- El gateway **ignora** los campos del handshake que no conoce.
- Lo nuevo se **añade** sin romper lo existente.
- La versión mayor **solo sube** si se quita algo o cambia lo que algo hace.

---

## Índice

0. [Descubrir el gateway y emparejarse (o iniciar sesión)](#0-descubrir-el-gateway-y-emparejarse)
1. [Conexión y handshake](#1-conexión-y-handshake)
2. [Mensaje ready](#2-mensaje-ready)
3. [Enviar audio de micrófono](#3-enviar-audio-de-micrófono)
4. [Enviar texto](#4-enviar-texto)
5. [Recibir tokens de texto](#5-recibir-tokens-de-texto)
6. [Recibir audio TTS](#6-recibir-audio-tts)
7. [Barge-in e interrupciones](#7-barge-in-e-interrupciones)
8. [Estado de servicios](#8-estado-de-servicios)
9. [Mensajes de error](#9-mensajes-de-error)
10. [Turns iniciados por el agente (push)](#10-turns-iniciados-por-el-agente-push)
11. [Modos de operación](#11-modos-de-operación)
12. [Referencia completa de mensajes](#12-referencia-completa-de-mensajes)
13. [Timeouts y cierre de sesión](#13-timeouts-y-cierre-de-sesión)
14. [Conversaciones](#14-conversaciones)
15. [Diagnóstico del pipeline (`pipeline_event`)](#15-diagnóstico-del-pipeline-pipeline_event)

---

## 0. Descubrir el gateway y emparejarse

Antes de la sesión, tu cliente necesita dos cosas: **dónde está el gateway** y **su `client_key`**. Las dos pueden venir de la configuración (un administrador creó el cliente con `fogar-gw clients add` y te pasó la clave), pero lo cómodo es que el cliente las consiga solo. Esta sección es lo que tiene que implementar.

**Dos clases de cliente (4.0).** Un aparato **del hogar**, de uso compartido (una tele, un altavoz), **se empareja** con un código: no se sabe quién habla y lo que se dice es del hogar. Un aparato **personal** (una app en el móvil de alguien, su Mac) **inicia sesión** con la cuenta de su persona: lo que se dice es privado de ella y ve también las conversaciones del hogar. Elige una de las dos; un aparato no cambia de clase (para pasar de una a otra, retira la clave y haz lo otro).

### Encontrar el gateway

Un gateway se identifica en un endpoint público, sin autenticación:

```http
GET http://<host>:8004/.well-known/fogar-gateway
```

```json
{"service": "fogar-gateway", "version": "1.29.0", "instance_id": "3f9a1c2e7b5d4a60"}
```

Es un Fogar si responde 200 con `"service": "fogar-gateway"`. No basta con `/healthz`: su `{"status": "ok"}` lo devolvería cualquier servicio. El endpoint no dice el nombre del gateway (privacidad): ese nombre llega al canjear un código en `/pair`, en `gateway.name`, y es el que enseñar al usuario. `instance_id` cambia en cada arranque del proceso y sirve para comprobar que una dirección llega a este gateway; no es un secreto.

Una dirección es `host` o `host:puerto` (puerto 8004 por defecto), sin protocolo. En la LAN y en la tailnet se habla en claro (`http://`, `ws://`); detrás de un proxy con TLS, `https://` y `wss://`.

Si ya estás emparejado, usa la dirección que guardaste junto a la clave: la clave solo vale en el gateway que la emitió. Si no, busca en este orden y quédate con el primero que responda, con un plazo corto por intento (1–2 s):

1. **La dirección configurada**, si el usuario puso una.
2. **El mismo equipo:** `127.0.0.1:8004`. Vale igual para el gateway nativo y para Docker con el puerto publicado. Si tu cliente admite otro puerto por configuración, úsalo; no barras puertos. Un gateway que escucha solo en una IP de la LAN (`GATEWAY_HOST`) no responde aquí: ese caso cae en el paso 4.
3. **La tailnet, si hay Tailscale:** `tailscale status --json` lista los equipos (`Peer[*]`, con `Online`, `DNSName` y `TailscaleIPs`). Pregunta en `:8004` a cada equipo en línea, por su `DNSName` sin el punto final, en paralelo y con un plazo total de unos 3 s. Si `tailscale` no está o no tiene sesión, salta este paso. Si responde más de uno, deja elegir al usuario por su `DNSName` (o la IP), que es lo que el discovery devuelve: el endpoint no trae nombre (ver arriba).
4. **Si no aparece ninguno,** pide la dirección a mano y compruébala igual, con `/.well-known/fogar-gateway`.

No escanees redes ni rangos de IP.

### Emparejarse

El administrador abre un código en el gateway con `fogar-gw pair` (ver el [README](../README.md#emparejar-un-cliente)). El gateway lo enseña: 6 cifras, de un solo uso y válido 10 minutos. En la configuración del código el administrador ya ha fijado cómo se comportará tu cliente: agente, voz, barge-in…

Tu cliente necesita **una pantalla de emparejamiento donde la persona solo escribe el código**. Todo lo demás lo pone el cliente:

```http
POST http://<host>:8004/pair
Content-Type: application/json

{"code": "482913", "name": "Tele del salón", "client_type": "screen", "output_mode": ["audio", "text", "status"]}
```

| Campo | Requerido | Quién lo pone |
|-------|-----------|---------------|
| `code` | ✓ | La persona. El gateway ignora los espacios (`"482 913"`); comprueba en el cliente que quedan 6 cifras antes de enviarlo, para no gastar un intento en una errata |
| `name` | — | El cliente propone uno (el nombre del dispositivo o el que elija el usuario, si le dejas). Si el administrador fijó otro, gana el suyo; si nadie da uno, el gateway pone `<tipo>-xxxx` |
| `client_type` | — | Fijo en tu cliente: `ha`, `esp32`, `web`, `app` o `screen` |
| `output_mode` | — | Fijo en tu cliente: lo que sabe reproducir (`"audio"`, `"text"`, `"status"`, como en §1). Se guarda en la ficha del cliente para el administrador; en cada sesión manda el `output_mode` del handshake |

Regla: **el cliente dice qué es** (tipo y formatos) **y el administrador decide cómo se comporta**. Lo que fijó el administrador al abrir el código manda sobre lo que mandes. El handshake de cada sesión (§1) sigue llevando `input_mode` y `output_mode`; no mandes `agent` salvo que el usuario elija uno, para que valga el que fijó el administrador.

| Respuesta | Qué significa |
|-----------|---------------|
| `201 {"client_id", "name", "client_key", "gateway": {"name", "version"}}` | Emparejado. **`client_key` solo llega aquí, una vez:** guárdala ya junto a la dirección del gateway, con permisos 0600 si va a un fichero, y nunca la escribas en un log. `name` es el nombre con el que quedó dado de alta (puede no ser el que propusiste) |
| `400 {"detail": "invalid or expired code"}` | Código desconocido, caducado, ya usado o mal escrito (siempre el mismo mensaje). Deja corregirlo |
| `422` | `code` no es un texto de hasta 32 caracteres (mándalo como texto, no como número), o `name` (1–200 caracteres), `client_type` u `output_mode` no válidos: es un fallo de tu cliente, no del usuario. No cuenta como intento |
| `413 {"detail": "request body too large"}` | El cuerpo pasa de 1 MiB (todas las peticiones HTTP del gateway tienen ese tope). Fallo de tu cliente; no cuenta como intento |
| `5xx` o sin respuesta | Si el gateway falló al crear el cliente, el código sigue abierto: reintenta con el mismo código. Si se cortó la conexión después del `201`, el código ya está gastado y la clave perdida: pide otro código |

Después abre la sesión normal (§1) con esa `client_key`.

**Intentos fallidos:** mientras haya códigos abiertos, cada `400` cuenta (un código caducado o ya usado también), sea de quien sea, y al quinto seguido se anulan todos los abiertos (un canje correcto pone la cuenta a 0); el administrador tiene que abrir otro. Es la defensa contra quien intente adivinar, porque `/pair` puede estar expuesto a internet a través de un proxy. El cliente no puede saber cuántos intentos quedan: si tras un `400` el usuario está seguro del código, dile que pida uno nuevo.

### Iniciar sesión (cliente personal, 4.0)

Para un aparato de una persona: la persona escribe **su usuario y su contraseña** una vez, y el cliente guarda el token que recibe.

```http
POST https://<host>/auth/login
Content-Type: application/json

{"username": "ana", "password": "…", "device_name": "iPhone de Ana", "client_type": "app", "capabilities": ["screen", "mic", "speaker"]}
```

| Respuesta | Qué significa |
|-----------|---------------|
| `201 {"client_id", "client_key", "user": {"id", "username", "display_name", "role"}, "home": {"id", "name"}}` | Dentro. `client_key` es el token de este aparato: **solo llega aquí, una vez**. Guárdalo como una contraseña (en el Llavero, nunca en un log) y úsalo como `client_key` en el handshake (§1) y como `Authorization: Bearer` en `/auth` y `/admin` |
| `401 {"detail": "invalid_credentials"}` | Usuario o contraseña incorrectos, o cuenta desactivada (siempre el mismo) |
| `403 {"detail": "insecure_transport"}` | Por HTTP desde la red de casa la contraseña iría en claro: el gateway solo la acepta por HTTPS, o por HTTP desde su propia máquina o la tailnet. Usa la URL `https://` del gateway |
| `429 {"detail": "too_many_attempts"}` + `Retry-After` | Demasiados fallos seguidos (por IP o por cuenta): espera esos segundos |

**El token no caduca ni se refresca:** vale hasta que se retira. `GET /auth/me` (con Bearer) dice quién eres y tu rol; `POST /auth/logout` retira este aparato (al cerrar sesión, **borra también lo que tengas en caché de esa persona**: un iPad puede ser compartido); `GET /auth/devices` y `DELETE /auth/devices/{id}`, los aparatos de la persona. Una contraseña nunca va en el handshake ni se guarda en el cliente.

### Cuando la clave deja de valer

Si al abrir la sesión el gateway cierra con **4010**, o una sesión abierta se cierra con **4200** (§13), la clave ya no vale: el administrador retiró, desactivó o rotó tu cliente o, si es personal, la persona cerró sesión en él, lo retiró desde otro aparato o la desactivaron. Mira el código, no el texto del `reason`. Borra la clave guardada y vuelve a la pantalla de emparejamiento (o, si es personal, a la de iniciar sesión). Ningún otro cierre (handshake mal formado, agente no permitido, conversación no encontrada, conversación abierta en otra sesión) invalida la clave. No reintentes en bucle con la misma clave.

---

## 1. Conexión y handshake

**Endpoint WebSocket:** `ws://<host>:8004/ws/stream`

El **primer mensaje** que envíes DEBE ser el handshake. Es el único mensaje sin campo `type`.

```json
{
  "client_key": "tu-api-key",
  "protocol": 5,
  "input_mode": "audio",
  "output_mode": ["audio", "text", "status"],
  "agent": "main"
}
```

| Campo | Tipo | Requerido | Descripción |
|-------|------|-----------|-------------|
| `client_key` | string | ✓ | Clave del dispositivo: la de un dispositivo **emparejado** (`/pair`, del hogar) o el token de uno **con sesión** (`/auth/login`, personal; 4.0). La web entra igual: ya no hay `admin_token` |
| `protocol` | int | ✓ | Versión del protocolo que implementa el cliente: `5`. Sin él, o con otra versión, el gateway cierra con 4003 |
| `capabilities` | string[] | — | Lo que sabe hacer el aparato (`screen`, `mic`, `speaker`…; minúsculas, cifras, `_` y `-`, hasta 32 de hasta 32 caracteres). El gateway las guarda y las enseña; aún no decide nada con ellas. Sin el campo se conservan las que tenía |
| `input_mode` | `"audio"` \| `"text"` | ✓ | Cómo enviará datos este cliente |
| `output_mode` | array | ✓ | Qué quiere recibir: `"text"` (mensajes `token`), `"audio"` (frames TTS), `"status"` (mensajes `turn_progress`, §5, y `pipeline_event`, §15). Los `status` de servicios, `error`, `transcription*` y el control de turnos llegan siempre |
| `agent` | string | — | Agente OpenClaw a usar; omitir (o vacío) para usar el agente por defecto (ver "Selección de agente" abajo). Un id de OpenClaw: letra o cifra y luego letras, cifras, `_` o `-`, hasta 64; otro valor (con `:`, espacios…) es 4000. Sin distinguir mayúsculas: el gateway lo pasa a minúsculas, como OpenClaw (`"Main"` es `main`, y así llega en `ready.agent`) |
| `conversation` | string (1–64) | — | La conversación de la sesión (§14): el id de una conversación **de este cliente** para retomarla, o `"new"` para empezar una. Sin él, la conversación del día. Nuevo en M19 |

### Selección de agente

El agente efectivo de la sesión no es simplemente "el que pediste o el global": el gateway resuelve una cascada server-side (`agent` del handshake si no está vacío → `default_agent` configurado por el admin para tu `client_key` → el agente que el hogar da a tu persona (dispositivo con sesión) o a lo común (dispositivo del hogar; 4.0) → `DEFAULT_AGENT`, el de la instalación (#245) → default global de OpenClaw → `"main"`) y aplica dos comprobaciones de política antes de aceptar la conexión: si tu cliente tiene una lista `allowed_agents`, el agente resultante debe estar en ella (una lista vacía lo deniega todo); y si pediste el agente explícitamente, debe existir en el roster de OpenClaw (`src/fogar_gateway/core/agent_policy.py`). Como cliente necesitas saber tres cosas:

1. Si omites `agent` en el handshake, el que acabes usando puede venir de una configuración por-cliente que un admin te haya asignado (no siempre el default global) — comprueba siempre `ready.agent`, no asumas cuál será.
2. Pedir explícitamente un `agent` puede fallar de dos formas distintas (ver tabla siguiente): que ese agente no esté permitido para tu cliente, o que no exista en OpenClaw.
3. Una conversación existente (`conversation` con un id) ya tiene su agente: es el de la sesión. No hace falta mandar `agent`; si lo mandas y es otro, el gateway cierra con 4101 (`conversation agent mismatch`). A ese agente se le aplican las mismas comprobaciones (lista permitida y roster).
4. El agente se fija al abrir la sesión: una sesión que dura días lo conserva al cambiar de día, y cambiar el `default_agent` del cliente sin quitar el agente actual de `allowed_agents` no le afecta hasta que reconectes. Si el administrador sí lo quita, la sesión se cierra con 4020 y los push de ese agente (de otra conversación) dejan de llegarte.

### Errores de handshake (el servidor cierra la conexión)

El código es el contrato; el `reason` es un texto en inglés para logs y personas, que puede cambiar: no lo compares. La tabla completa de cierres, con qué hacer en cada uno, está en la §13 («Códigos de cierre»).

| Situación | Código WS | `reason` |
|-----------|-----------|----------|
| JSON inválido (también un número de más de 4300 cifras, que antes cerraba con 1006; solo con `HANDSHAKE_MAX_BYTES` por encima de ~4300: con el valor por defecto cierra antes con 4002), que no es un objeto, o campos incorrectos (también un `agent` que no es un id de agente válido); sin `client_key`, o unas `capabilities` mal formadas | 4000 | `invalid handshake` |
| Falta `protocol` o no es `5` (se mira antes que cualquier otro campo; un JSON que no es objeto sigue siendo 4000) | 4003 | `protocol version not supported` |
| El primer mensaje es binario, no texto | 4001 | `handshake must be a text frame` |
| El handshake supera `HANDSHAKE_MAX_BYTES` (4096 caracteres) | 4002 | `handshake too large` |
| No se recibe el handshake JSON en los primeros `HANDSHAKE_TIMEOUT_S` (5s por defecto) | 4900 | `handshake timeout` |
| `client_key` inválida, cliente inactivo o retirado, o usuario desactivado (dispositivo con sesión) | 4010 | `invalid or inactive credential` |
| El agente efectivo no está en la lista permitida para este cliente (`allowed_agents`) | 4020 | `agent not permitted` |
| El agente pedido no existe en OpenClaw | 4021 | `agent not available` |
| `conversation` no existe o es de otro cliente (no se distingue) | 4100 | `conversation not found` |
| `agent` distinto del de la conversación pedida | 4101 | `conversation agent mismatch` |
| Fallo al abrir los servicios internos de la sesión (también al abrir la conversación) | 1011 | `session setup failed` |
| Orquestador no disponible tras health check | 1011 | `orchestrator unavailable` |

> **El orquestador (OpenClaw) es el único servicio que puede cerrar la conexión en el handshake.** Transcriber y TTS ya **no** son críticos: si cualquiera de los dos no responde al arrancar la sesión, el gateway la abre igualmente en modo degradado y te lo notifica vía `status` (ver §8) — nunca cierra el WebSocket por esto. Antes de la versión que introdujo la reconexión automática (issue #46), un fallo de Transcriber en este punto sí cerraba la conexión con 1011; ya no es el caso.
>
> Estos mensajes `status` de arranque pueden llegar **antes** del `ready` (el health check se ejecuta justo antes de enviarlo) — no asumas que `ready` es el primer mensaje posible tras el handshake, solo que es el primero que confirma que la sesión quedó establecida.

---

## 2. Mensaje ready

Si el handshake es válido y todos los servicios críticos responden, el gateway envía `ready` como el mensaje que confirma la sesión; pueden llegar `status` antes (ver nota en §1 y §5):

```json
{
  "type": "ready",
  "protocol": 5,
  "session_id": "hab_sito:1751289600000",
  "agent": "main",
  "conversation": {"id": "0b9d6c1e-5f3a-4c2e-9d8b-2a7c1e4f6a90", "agent": "main"},
  "input_mode": "audio",
  "output_mode": ["audio", "text"],
  "requested_capabilities": {
    "barge_in": true,
    "tts": true,
    "transcriber": true
  },
  "live_capabilities": {
    "barge_in": true,
    "tts": true,
    "transcriber": true
  }
}
```

| Campo | Descripción |
|-------|-------------|
| `protocol` | Versión del protocolo que habla el gateway (`5`) |
| `session_id` | Identificador único de esta sesión (`client_id:timestamp_ms`) |
| `agent` | Agente OpenClaw activo (el solicitado o el por defecto; el de la conversación) |
| `conversation` | La conversación de la sesión (§14): `id` (guárdalo para retomarla con `conversation` en el handshake) y `agent`. Nuevo en M19. En una sesión sin conversación elegida puede cambiar a mitad de sesión con el día: mira `turn_start.conversation` |
| `input_mode` | Modo de entrada confirmado |
| `output_mode` | Modos de salida activos |
| `requested_capabilities.barge_in` | Si el barge-in está habilitado para este cliente |
| `requested_capabilities.tts` | Si el cliente pidió salida de audio (`"audio"` en `output_mode`) |
| `requested_capabilities.transcriber` | Si el cliente pidió modo audio (`input_mode == "audio"`) |
| `live_capabilities.barge_in` | Si el barge-in está operativo (requiere barge-in habilitado, transcriber activo y un proveedor STT con parciales) |
| `live_capabilities.tts` | Si TTS responde en este momento |
| `live_capabilities.transcriber` | Si el transcriptor está conectado (`input_mode == "audio"`) |

Espera este mensaje antes de enviar audio o texto. Si no llega, la conexión fue cerrada por un error de handshake.

> **Voz desactivada.** Si el gateway tiene la voz en `off` (`STT_PROVIDER`/`TTS_PROVIDER`), se comporta como con ese servicio caído, y solo para lo que pidió la sesión: antes de `ready` llega `status` con `state: "unavailable"` para `transcriber` si `input_mode` es `"audio"`, y para `tts` si `output_mode` incluye `"audio"`; una sesión solo de texto no recibe ninguno. `live_capabilities.transcriber`/`tts` (y `barge_in`) valen `false`. La sesión sigue abierta en texto: `send` funciona, el audio que mandes se descarta y no hay avisos de recuperación.

> **Incompatibilidad de protocolo — #114 (v1.15.0)**
> `ready.capabilities` fue reemplazado por `ready.requested_capabilities` + `ready.live_capabilities`.
> `requested_capabilities` refleja lo que el cliente pidió en el handshake.
> `live_capabilities` refleja el resultado del health check en el momento de la conexión.
> Clientes que aún lean `capabilities` dejarán de encontrar el campo — deben actualizarse.

## 3. Enviar audio de micrófono

Requiere `input_mode: "audio"` en el handshake.

Envía frames binarios de audio crudo con este formato exacto:

| Parámetro | Valor |
|-----------|-------|
| Formato | PCM Float32 little-endian |
| Sample rate | 16 000 Hz |
| Canales | 1 (mono) |
| Header | ninguno — solo los bytes de audio |

### Flujo de voz completo

```
Cliente                          Gateway
  │─── [PCM Float32] ──────────►│
  │─── [PCM Float32] ──────────►│
  │◄── {"type":"transcription_partial","text":"enciende la..."} ─│
  │─── [PCM Float32] ──────────►│
  │─── {"type":"end"} ─────────►│   ← usuario suelta el micrófono
  │◄── {"type":"transcription","text":"enciende la luz"} ────────│
  │                              │   ← cliente muestra texto para revisión
  │─── {"type":"send","text":"enciende la luz"} ───────────────►│
  │◄── {"type":"message","id":"…","text":"enciende la luz","from":{…,"self":true},"state":"delivered",…} ─│
  │◄── {"type":"turn_start","turn_id":"t-1","turn_seq":1,"conversation":"…","origin":"user","by":{…}} ─│
  │◄── {"type":"token","turn_id":"t-1","text":"Vale,"} ──────────│
  │◄── [0xA1][0x00][0x01][PCM16...] ────────────────────────────│
  │◄── {"type":"token","turn_id":"t-1","text":" encendida."} ────│
  │◄── [0xA1][0x00][0x01][PCM16...] ────────────────────────────│
  │◄── {"type":"turn_end","turn_id":"t-1","reason":"done"} ──────│
```

### Señal de fin de audio

Envía `{"type": "end"}` cuando el usuario termine de hablar:

```json
{ "type": "end" }
```

Úsalo cuando el usuario suelta el botón de micrófono, detectas silencio prolongado en cliente, o el VAD local decide que la frase terminó. Tras `end`, el transcriptor entrega la transcripción final y el cliente espera para enviarla con `send`.

---

## 4. Enviar texto

### Mensaje send (modo canónico)

Envía texto al orquestador — ya sea una transcripción confirmada o texto escrito directamente:

```json
{ "type": "send", "text": "¿Cuál es la capital de Francia?" }
```

El campo `text` puede diferir de la transcripción original si el usuario la editó.

**El mensaje entra en la cola de OpenClaw (5.0).** Un `send` con un turno en curso en la conversación (tuyo, de otro aparato o un push) **no lo corta**: tu mensaje espera su turno en OpenClaw (`messages.queue`) y lo ves como `message` `pending` (§5); cuando le toca, pasa a `delivered` y su turno se abre. Dos `send` de dos aparatos a la vez van a OpenClaw en el orden en que llegan al gateway; nunca reciben `turn_in_progress`. **Excepción, Home Assistant (`/v1`, 5.0):** su mensaje nunca se mezcla con otro turno: espera a que la conversación quede libre y, mientras su turno sigue en marcha, un `send` tuyo espera en el gateway (en orden de llegada) y se envía cuando ese turno termina; su `message` te llega entonces. Todo `send` aceptado por OpenClaw te llega primero como `message` (también a las demás sesiones de la conversación); su par `turn_start`/`turn_end` llega cuando su run empieza.

**Enviar ahora:** con `"interrupt": true` el gateway corta antes el turno en curso de la conversación (solo ese: aborta su run por su id, sin vaciar la cola), que termina con `turn_end` `interrupted` en todas las sesiones, y después envía el tuyo:

```json
{ "type": "send", "text": "Mejor dime la de Italia", "interrupt": true }
```

`interrupt` es opcional (por defecto `false`); si viene y no es un booleano, recibes `INVALID_MESSAGE` y no se envía nada. Lo que siga en la cola de OpenClaw (mensajes pendientes de otros) sigue en ella, delante del tuyo.

> **Incompatibilidad de protocolo — 5.0**
> Un `send` ya no sustituye el turno en curso (antes lo cerraba con `turn_end` `replaced` y abortaba todo lo que corría en la sesión de OpenClaw): espera en la cola. Para cortar y enviar, `"interrupt": true`. Un `send` ya no produce siempre su par `turn_start`/`turn_end`: produce su `message`, y su par llega si su run abre turno (un mensaje que quitas o paras antes de empezar no lo tiene, §5).

`send` es la única forma de enviar un prompt, en cualquier `input_mode`. Desde 5.0 un frame de texto que no es JSON (`hola`) ya no arranca un turno, ni siquiera en modo texto: recibe `INVALID_MESSAGE` (§9) y la sesión continúa.

Un surrogate UTF-16 suelto escapado en `text` (`"\ud800"`, JSON válido pero no UTF-8) se quita antes de enviarlo al orquestador; lo mismo con el último mensaje del usuario en `/v1/chat/completions`, con el texto que llega de OpenClaw y con cualquier mensaje que el gateway te envíe (una transcripción, por ejemplo). Se quita sin sustituto: un emoji que OpenClaw parte entre dos fragmentos no hace que se repita el texto.

### Cancelar turno activo

Si el usuario cancela la respuesta en curso sin lanzar una nueva:

```json
{ "type": "cancel" }
```

Para el turno en curso de la conversación, lo haya iniciado quien lo haya iniciado (otra sesión, otro aparato): todas las sesiones reciben su `turn_end` `cancelled`. Solo se aborta su run; los mensajes pendientes siguen en la cola. También corta un push que esté sonando (§10): recibes su `turn_end`. Si no hay turno abierto pero el último mensaje entregado aún no ha empezado su run, lo para: queda `withdrawn` (sin par).

### Quitar un mensaje pendiente (5.0)

Cualquier sesión de la conversación puede quitar un mensaje que aún espera su turno (`message` `pending`, §5), lo enviara quien lo enviara:

```json
{ "type": "withdraw", "message_id": "<id del message>" }
```

El gateway aborta en OpenClaw solo ese mensaje (su run, por su id) y todas las sesiones reciben su `message` con `state: "withdrawn"`. Si el mensaje ya no está pendiente (ya entregado, ya quitado, de otra conversación o desconocido), **no pasa nada** (ni error ni aviso): otra sesión pudo quitarlo o ya le tocó su turno. Sin `message_id` de texto, `INVALID_MESSAGE`.

---

## 5. Recibir tokens de texto

Requiere `"text"` en `output_mode`.

Cada turno consiste en tres mensajes en este orden:

### `turn_start` — inicio de turno

```json
{ "type": "turn_start", "turn_id": "t-1", "turn_seq": 1, "conversation": "0b9d6c1e-…", "origin": "user",
  "by": { "device_id": "salon-tv", "name": "Tele del salón", "self": false } }
```

| Campo | Descripción |
|-------|-------------|
| `turn_id` | Identificador del turno (`t-{N}`, secuencial por sesión desde 1) |
| `turn_seq` | N módulo 65 536 (uint16, no monótono tras el desbordamiento; compáralo por igualdad, no por orden) — es el mismo valor que lleva el header de los frames de audio |
| `conversation` | La conversación del turno (§14). En un turno tuyo, la de la sesión en ese momento (cambia con el día si no elegiste conversación); en un push, la del push, que puede no ser la de la sesión (un recordatorio de la conversación de ayer). `null` en un push de una sesión de OpenClaw que el gateway no conoce. Nuevo en M19 |
| `origin` | `"user"` (lo abrió un `send`, tuyo o de otra sesión de la conversación) o `"push"` (lo abrió el agente, §10). Nuevo en M19 |
| `by` | Quién lo inició: `{"device_id", "name", "self"}` (el aparato que envió el mensaje, su nombre y si es este mismo aparato), o `null` en un push. Nuevo en 5.0 |

### `token` — texto en streaming

```json
{ "type": "token", "turn_id": "t-1", "text": "La capital" }
{ "type": "token", "turn_id": "t-1", "text": " de Francia" }
{ "type": "token", "turn_id": "t-1", "text": " es París." }
```

Los tokens llegan en orden; concaténalos para reconstruir la respuesta completa. El campo `turn_id` permite asociar tokens con su `turn_start` aunque lleguen entrelazados con frames de audio.

### `turn_end` — fin de turno

```json
{ "type": "turn_end", "turn_id": "t-1", "reason": "done" }
```

Señala que el turno terminó. **Todo turno con `turn_start` recibe exactamente un `turn_end`** en tu socket — también si falla (tras un `error`), si se cancela (`cancel`, barge-in, transcripción final, un `send` que interrumpe) o si el cierre de sesión lo corta. Desde 5.0 un `send` aceptado produce su `message` (abajo) y su par `turn_start`/`turn_end` cuando su run empieza: dos `send` seguidos dan dos pares, uno detrás del otro (los dos `done`); un mensaje quitado (`withdraw`) o parado antes de empezar no tiene par. Un `send` que OpenClaw no acepta (sin conexión, rechazado) recibe su par con su `error` `TURN_ERROR`, solo en tu sesión. Después del `turn_end` no se envía más audio de ese turno; lo que sigue es la reproducción del audio que ya recibiste (como mucho `AUDIO_LEAD_S`, unos 2 s, por delante del tiempo real).

`reason` (nuevo en M19) dice por qué terminó:

| `reason` | Cuándo |
|---|---|
| `done` | Terminó solo: la respuesta completa (o el push, con su fin) |
| `error` | Falló: va precedido de su `error` `TURN_ERROR` (§9), también un push cuyo agente falla (desde 5.0, #397) |
| `cancelled` | Un `cancel` de cualquier sesión de la conversación (§4), o OpenClaw abortó el run por su cuenta. Un mensaje parado antes de que empiece su run no tiene par: su `message` queda `withdrawn` (5.0) |
| `interrupted` | Un `send` con `interrupt` lo cortó («enviar ahora», también un push), o hablaste encima: un barge-in (§7) o una transcripción final con el turno en curso. Sustituye al antiguo mensaje `interrupted` |
| `replaced` | Tu sesión pasó a la conversación del día nuevo (§14), o un `send` tuyo que no llegó a la conversación dejó atrás el turno que mostrabas. Desde 5.0 un `send` ya no sustituye el turno en curso |
| `timeout` | `TURN_TIMEOUT_S` sin progreso del orquestador (§13), tras su `error` `turn_timeout` |
| `closed` | La sesión se cierra (o te fuiste) con el turno abierto |

Trata cualquier otro valor como `error`: la lista puede crecer.

**Varias sesiones en una conversación (5.0).** Si la conversación está abierta en otras sesiones a la vez (otra pestaña, otro aparato; §14), cada turno de la conversación llega a cada una con su propio par: su `turn_id` y su `turn_seq` (cada sesión numera los suyos), su `turn_start` y su `turn_end`, y su texto según su `output_mode`. El audio solo suena en la sesión que envió el turno (en un push, en una sola). Una sesión que entra con un turno ya en marcha recibe su `turn_start` y, si pide `"text"`, un único `token` con lo que el turno llevaba dicho; después, lo que siga (sin audio). Que una sesión se vaya (o se cierre) a mitad de un turno no lo corta para las demás: recibe su propio `turn_end` (`closed`) y el turno sigue en las otras.

### `message` — los mensajes de la conversación (nuevo en 5.0)

Cada mensaje de usuario que OpenClaw acepta en la conversación llega a **todas** las sesiones adjuntas, también a la que lo envió, con su estado, y otra vez cada vez que cambia:

```json
{ "type": "message", "id": "6f1c…", "conversation": "0b9d6c1e-…", "text": "¿Y mañana?",
  "from": { "source": "device", "device_id": "salon-tv", "name": "Tele del salón", "self": false },
  "state": "pending" }
```

| Campo | Descripción |
|-------|-------------|
| `id` | Id del mensaje (el del run de OpenClaw). Es el `message_id` de `withdraw` (§4) |
| `conversation` | La conversación del mensaje (§14) |
| `text` | Lo que se envió (tal cual llegó, tras quitar surrogates sueltos). Texto del usuario: no lo registres |
| `from` | De dónde vino: `source` (`"device"` hoy; puede crecer, trata otro valor como un origen genérico), `device_id`, `name` y `self` (si lo envió este mismo aparato) |
| `state` | `pending` (espera su turno en la cola de OpenClaw), `delivered` (le llegó al agente: empieza su turno, o se lo incorporó al turno en curso) o `withdrawn` (quitado con `withdraw`, o parado antes de empezar) |

Un mensaje sin nada delante llega directamente `delivered`; uno enviado con un turno en curso, `pending`, y después `delivered` (justo antes del `turn_start` de su turno) o `withdrawn`. Si OpenClaw lo incorpora al turno en curso (su modo `steer`, el de por defecto), pasa a `delivered` en cuanto lo incorpora y **no tiene turno propio**: su respuesta llega dentro de ese turno, que sigue siendo de quien lo inició (su `by`, su audio). Como OpenClaw solo avisa de que lo incorporó después de hacerlo, un `withdraw` en ese intervalo puede dejarlo `withdrawn` aunque su respuesta llegue igualmente dentro del turno en curso. Un pendiente que falla antes de empezar (un error del agente, o una caída del orquestador tras la que ya no está en su cola) no desaparece: pasa a `delivered` y recibe su par con un `error` `TURN_ERROR` (§9). Llega por el orden total de tu sesión: antes que el turno que lo responde. Lo que Home Assistant manda por `/v1` a una conversación que tienes abierta también te llega así (`from.device_id`/`name` del cliente de Home Assistant, `self: false`) y su turno es uno más (`turn_start.by` = ese aparato), sin audio para nadie: nunca está `pending`, porque `/v1` espera a que la conversación quede libre antes de enviarlo. Una sesión que se adjunta con mensajes pendientes recibe, tras `ready` (y tras el `turn_start` del turno en curso, si lo hay), un `message` `pending` por cada uno.

### `tool_call` — uso de herramientas del agente (opt-in)

Solo se envía si el cliente tiene el flag `tool_calls_enabled` activado (por defecto `False`, gestionable vía Admin API — ver el [README](../README.md#admin-requiere-x-admin-token)). Cuando el agente invoca una herramienta durante el turno, llegan dos mensajes intercalados con los `token`:

```json
{ "type": "tool_call", "turn_id": "t-1", "phase": "start", "name": "exec", "tool_call_id": "call-1", "args": {"command": "ls"}, "result": null, "is_error": null }
{ "type": "tool_call", "turn_id": "t-1", "phase": "result", "name": "exec", "tool_call_id": "call-1", "args": null, "result": "file.txt", "is_error": false }
```

| Campo | Descripción |
|-------|-------------|
| `turn_id` | Turno al que pertenece (mismo `turn_id` que su `turn_start`) |
| `phase` | `"start"` (la herramienta empieza a ejecutarse) o `"result"` (resultado disponible) |
| `name` | Nombre de la herramienta |
| `tool_call_id` | Identificador único de esta invocación — correlaciona `start` con su `result` |
| `args` | Argumentos pasados a la herramienta (solo en `phase="start"`) |
| `result` | Resultado en texto plano (solo en `phase="result"`) |
| `is_error` | Si la herramienta falló (solo en `phase="result"`) |

No hay un `phase="update"` — OpenClaw emite resultados parciales de streaming que el gateway descarta deliberadamente; solo se reenvían inicio y resultado final. También llegan durante turnos iniciados por el agente (push, ver §10).

### `turn_progress` — en qué está el turno (nuevo, #254)

Solo con `"status"` en `output_mode`. Mientras el agente prepara la respuesta o usa herramientas, el cliente no recibe nada más: con OpenClaw 2026.10.5 el texto llega casi entero al final, y una respuesta con herramientas (Home Assistant, MCP) puede tardar decenas de segundos. `turn_progress` dice en qué está, para que el cliente lo enseñe («Pensando…», «Usando ha_call_service…») o suene algo en lugar de silencio:

```json
{ "type": "turn_progress", "turn_id": "t-1", "phase": "preparing", "tool": null }
{ "type": "turn_progress", "turn_id": "t-1", "phase": "thinking", "tool": null }
{ "type": "turn_progress", "turn_id": "t-1", "phase": "tool", "tool": "ha_call_service" }
{ "type": "turn_progress", "turn_id": "t-1", "phase": "thinking", "tool": null }
```

| Campo | Descripción |
|-------|-------------|
| `turn_id` | Turno al que pertenece (el de su `turn_start`) |
| `phase` | `"preparing"`: el orquestador prepara la ejecución (espacio de trabajo, contexto, memoria). `"thinking"`: el modelo está generando, también al volver de una herramienta. `"tool"`: el agente usa una herramienta |
| `tool` | Con `phase: "tool"`, el **nombre** de la herramienta (con varias a la vez, la última que sigue abierta); `null` en las demás fases y cuando el nombre no es un identificador (`[A-Za-z0-9_.:-]`, hasta 128 caracteres): entonces es «una herramienta». Nunca lleva argumentos ni resultado |

Reglas:

- **Solo dentro de su turno:** llega después de su `turn_start` y antes de su `turn_end`, en el orden total de la sesión, nunca después del `turn_end`. Nada de una ejecución abortada (barge-in, `cancel`, un `send` que la sustituye) llega como progreso, ni nada del turno desde que se interrumpe aunque su `turn_end` tarde un momento. Vale igual para un push (§10): si el orquestador contó en qué estaba antes de abrirlo, ese último estado llega justo después de su `turn_start`.
- **Es el estado actual, no un registro:** cada uno sustituye al anterior. Un `token` lo deja obsoleto (el texto ya está llegando; bórralo); puede volver a llegar después de un `token` si el agente usa otra herramienta a media respuesta. Sin un `token` entre medias nunca llega el mismo estado dos veces seguidas, y llega como mucho uno cada 250 ms: si el orquestador encadena herramientas más deprisa, el gateway se salta los intermedios y manda el más reciente (y lo que quedara aplazado al salir un `token` ya no sale).
- **No es una garantía:** un turno puede no traer ninguno (una respuesta inmediata, o un orquestador que no lo cuenta), y no hay uno de cierre: el fin es el `turn_end`.
- **Independiente de `tool_calls_enabled`:** ese flag sigue gobernando el mensaje `tool_call` completo (con `args` y `result`); `turn_progress` solo lleva el nombre, así que llega con el flag desactivado.
- **Vocabulario del gateway:** el gateway traduce lo que cuenta el orquestador (las fases de arranque de OpenClaw y sus eventos de herramienta, ver [`openclaw-protocol.md`](openclaw-protocol.md)) a estas fases; nunca reenvía el texto del orquestador. La lista puede crecer: trata un `phase` que no conozcas como un «trabajando» genérico.

**Por qué esta forma.** Como `turn_start` y `turn_end`, es un mensaje del turno (prefijo `turn_` y su `turn_id`); `phase` es el estado actual y `tool` va siempre presente (`null` si no aplica), como los campos de `tool_call`. Va con `"status"` porque, como `pipeline_event`, cuenta cómo va la sesión sin ser contenido: un cliente que no lo pide (uno mínimo, la tele) no recibe nada nuevo.

> **Cambio aditivo — #254.** Mensaje nuevo, solo para clientes con `"status"` en `output_mode`; nada de lo que ya llegaba cambia. Un cliente que ignora los `type` que no conoce no tiene que hacer nada.

### Pseudocódigo de recepción

```python
current_buffer = {}

async for msg in ws:
    if isinstance(msg, str):
        data = json.loads(msg)
        match data["type"]:
            case "ready":
                session_id = data["session_id"]
                agent = data["agent"]
                requested = data["requested_capabilities"]
                live = data["live_capabilities"]

            case "message":
                # 5.0: cada mensaje de la conversación (también el tuyo) y su estado
                show_message(data["id"], data["text"], data["from"], data["state"])

            case "turn_start":
                turn_id = data["turn_id"]
                current_buffer[turn_id] = ""

            case "token":
                turn_id = data["turn_id"]
                current_buffer[turn_id] += data["text"]
                render_streaming(data["text"])

            case "turn_end":
                turn_id = data["turn_id"]
                text = current_buffer.pop(turn_id, "")
                if data["reason"] == "interrupted":
                    clear_current_response_ui()  # antes: el mensaje `interrupted`
                else:
                    on_turn_complete(text)

            case "tool_call":
                # Solo llega si tool_calls_enabled=True para este cliente
                handle_tool_call(data["turn_id"], data["phase"], data["name"], data.get("args"), data.get("result"))

            case "turn_progress":
                # Solo con "status" en output_mode: el estado actual del turno; un token lo deja obsoleto
                show_progress(data["turn_id"], data["phase"], data["tool"])

            case "transcription_partial":
                show_live_transcription(data["text"])

            case "transcription":
                show_final_transcription_for_review(data["text"])

            case "status":
                handle_service_status(data["service"], data["state"])

            case "error":
                handle_error(data["code"], data["message"], data["fatal"])

    elif isinstance(msg, bytes):
        play_audio_frame(msg)
```

### Orden total de mensajes

El gateway entrega todos los mensajes de una sesión en un único **orden total** — el mismo orden en que el gateway los encola internamente (issue #130 S1). Esto garantiza que nunca hay interleaving de mensajes parciales: un `token` completo llega antes del siguiente, un `turn_end` no se mezcla con el `token` anterior. Sin embargo, **`ready` no está garantizado que sea el primer mensaje**: tanto los `status` del health check como un `status` difundido a todas las sesiones por `ClientRegistry` mientras la sesión ya está registrada pero `ready` aún no se ha enviado, pueden preceder a `ready`. Los clientes deben estar preparados para recibir `status` antes de `ready`.

---

## 6. Recibir audio TTS

Requiere `"audio"` en `output_mode`.

Los frames de audio llegan como **mensajes binarios** intercalados con los JSON de control.

### Formato de los frames binarios

```
┌──────────┬──────────────────────┬─────────────────────┐
│  0xA1    │  turn_seq (uint16 BE) │  PCM16 24 kHz       │
│  1 byte  │  2 bytes              │  N bytes            │
└──────────┴──────────────────────┴─────────────────────┘
```

| Parámetro | Valor |
|-----------|-------|
| Magic byte | `0xA1` — identifica frames de audio |
| `turn_seq` | Uint16 big-endian — mismo valor que `turn_start.turn_seq`. Es el contador de turnos módulo 65 536: tras el turno 65 535 vuelve a 0 (el `turn_id` `t-N` no se reinicia), así que compara por igualdad y no asumas que crece siempre |
| Audio | PCM16 signed 16-bit little-endian, 24 000 Hz, 1 canal (mono) |

El `turn_seq` permite descartar audio de turnos anteriores: un frame cuyo `turn_seq` **no coincide** con el del último `turn_start` recibido no es del turno actual. Es módulo 65 536 y no es monótono tras el desbordamiento, así que **no compares "mayor/menor"**: compara por igualdad (o usa `turn_id` en los mensajes JSON).

### Ritmo y tamaño de los frames

El gateway envía el audio a ritmo de reproducción, con ~`AUDIO_LEAD_S` (2 s por defecto) de adelanto, en frames de ≤200 ms (9 600 bytes de PCM). No dependas del tamaño de los frames. `turn_end` llega ~2 s antes de que termine de sonar el audio: es el fin de la respuesta, no del sonido.

### Cómo parsear un frame

```python
def parse_audio_frame(data: bytes):
    if len(data) < 3 or data[0] != 0xA1:
        return None, None  # no es un frame de audio
    turn_seq = (data[1] << 8) | data[2]
    pcm16 = data[3:]
    return turn_seq, pcm16
```

### Pseudocódigo con barge-in por turn_seq

```python
current_turn_seq = 0
audio_buffer = bytearray()

async for msg in ws:
    if isinstance(msg, bytes):
        if len(msg) >= 3 and msg[0] == 0xA1:
            frame_seq = (msg[1] << 8) | msg[2]
            pcm16 = msg[3:]
            if frame_seq == current_turn_seq:
                # Audio del turno actual — reproducir. Los frames de otro turno
                # (anterior, ya cancelado) se descartan; nunca compares con > o <.
                audio_player.write(pcm16)

    elif isinstance(msg, str):
        data = json.loads(msg)
        if data["type"] == "turn_start":
            current_turn_seq = data["turn_seq"]
            audio_player.prepare_for_new_turn()
        elif data["type"] == "turn_end" and data["reason"] == "interrupted":
            audio_player.stop()
```

> Si el TTS no está disponible, el gateway continúa sin audio — los `token` siguen llegando igualmente. `live_capabilities.tts` en el `ready` te avisa de antemano.

---

## 7. Barge-in e interrupciones

El barge-in ocurre cuando el usuario empieza a hablar mientras el orquestador aún está respondiendo.

El gateway detecta la parcial de transcripción y cancela el turno activo si el barge-in está habilitado (`requested_capabilities.barge_in`; `live_capabilities.barge_in` indica si está operativo) y la parcial supera el umbral mínimo de caracteres. El cliente recibe el `turn_end` del turno cancelado con su motivo:

```json
{ "type": "turn_end", "turn_id": "t-1", "reason": "interrupted" }
```

El orden es `turn_end` (t-1, `interrupted`) → `turn_start` (t-2). Desde 5.0, un mensaje tuyo cortado así antes de que empiece su run no tiene par: su `message` queda `withdrawn` (§5).

> **Incompatibilidad de protocolo — M19**
> El mensaje `{"type":"interrupted","turn_id":...}` **ya no existe**: lo sustituye `turn_end.reason: "interrupted"` del mismo turno. Un cliente que esperaba `interrupted` para parar el audio o limpiar la respuesta debe mirar `reason` en el `turn_end`.

**Un push que está sonando también se corta** (§10): el barge-in, un `send` con `interrupt` y un `cancel` lo cierran igual que a un turno normal. Recibes su `turn_end` (exactamente uno) antes de cualquier otra cosa del turno nuevo, con `reason` `interrupted` o `cancelled`. Un `send` sin `interrupt` no lo corta (5.0): espera a que termine. Su audio deja de llegar. La transcripción final por sí sola no lo corta: lo corta el `send` que la sigue.

El audio del turno cancelado deja de salir en cuanto el gateway procesa la cancelación, sin esperar a que se libere su TTS, y su `turn_end` tampoco espera a que se cierre la conexión con el servidor de voz; lo que ya estaba en camino (como mucho `AUDIO_LEAD_S` por delante) puede llegar antes del `turn_end`. Nada del turno cancelado llega después de su `turn_end`: ni tokens ni audio, tampoco la respuesta que OpenClaw ya tuviera en camino al abortarlo, que no cierra tu turno siguiente ni vuelve como un push (#285).

Tras un `turn_end` con `reason: "interrupted"`:
- Limpia la UI de la respuesta en curso
- Descarta audio con `turn_seq` del turno cancelado (ya no llegará más)
- El flujo reinicia normalmente — sigue enviando audio

El nuevo turno llegará con un `turn_start` con un `turn_seq` distinto (módulo 65 536, no necesariamente mayor), lo que también activa el descarte de audio del turno anterior a nivel de frame (por igualdad de `turn_seq`).

---

## 8. Estado de servicios

El gateway notifica cambios en el estado de sus microservicios durante la sesión:

```json
{ "type": "status", "service": "tts", "state": "degraded" }
```

Los tres microservicios downstream (orquestador, Transcriber, TTS) tienen reconexión automática con backoff exponencial. Si el orquestador o el transcriber aceptan la conexión y la cortan enseguida (en menos de 10 s), el primer reintento es inmediato (un corte suelto no pierde la frase en curso), pero desde el segundo corte seguido el reintento espera, y esa espera se dobla en cada corte: los pares `reconnecting`/`restored` llegan cada vez más espaciados en vez de en ráfaga, y la espera vuelve a cero en cuanto una conexión aguanta. El vocabulario de conectividad es:

| `state` | Significado |
|---------|-------------|
| `unavailable` | El servicio está caído. Puede o no estar reintentando activamente en segundo plano (ver tabla siguiente). |
| `reconnecting` | Reintentando activamente ahora mismo. |
| `restored` | Volvió a la normalidad tras haber estado `unavailable`/`reconnecting`. |
| `degraded` | Solo para `transcriber` — señal de **calidad**, no de conectividad (silencio prolongado **mientras envías audio** — con un turno en curso o sin mandar audio, p. ej. sesión persistente en reposo con solo `keepalive`, no se cuenta silencio; buffer lleno, GPU saturada con `code: "gpu_saturated"`, proveedor externo sin capacidad momentánea con `code: "provider_capacity"`, o frase perdida con `code: "phrase_interrupted"`: la conexión con el transcriptor `fogar` se cortó o se estaba reabriendo mientras el usuario hablaba, y el gateway no entrega esa transcripción final (ni sus parciales), que llegaría sin su principio y podría decir lo contrario de lo dicho; pide al usuario que la repita. El audio que mandas mientras se reabre tras la final anterior no se pierde: el gateway lo guarda (hasta 320 KB, unos 5 s de audio Float32 a 16 kHz, sea cual sea el tamaño de tus tramas) y lo pasa a la conexión nueva, así que una frase que empieza ahí llega entera; solo avisa si esa frase termina, con su `end`, antes de que la conexión nueva esté lista, o si no cabe en ese tope (se descarta el audio que ya no cabe, nunca el de frases anteriores). Con el transcriptor `unavailable` no se guarda nada. Con el transcriptor `unavailable` no llega este aviso: ya sabes que no hay transcripción). No forma parte del ciclo unavailable→reconnecting→restored. Cuando la causa es saturación del transcriber (`gpu_saturated` o `provider_capacity`), el aviso de recuperación es `restored`; el resto de causas de `degraded` no tienen aviso de recuperación. |

Por servicio:

| `service` | Comportamiento e impacto en la sesión |
|-----------|-----------------------------------------|
| `orchestrator` | Reintenta en segundo plano con backoff mientras la sesión está activa. Si cae **antes o durante el handshake** (health check inicial), es el único caso fatal: el gateway cierra el WebSocket con 1011 y nunca llega a enviar `ready` (ver §1). Si cae **durante una sesión ya establecida**, el WebSocket **no se cierra** — solo falla el turno en curso (`error` con `code: "TURN_ERROR"`, `fatal: false`) y **todas** las sesiones conectadas reciben proactivamente `status: {orchestrator, reconnecting}` al caer y `status: {orchestrator, restored}` al volver, incluso si están inactivas en ese momento (no hace falta intentar un turno para enterarte). **Una sola fase de reintento, sin tope (#272):** el gateway reintenta sin parar (backoff exponencial acotado por `ORCHESTRATOR_RECONNECT_MAX_BACKOFF`, 60 s por defecto), así que tras una caída larga el estado sigue siendo `reconnecting` —nunca pasa a `unavailable` por llevar mucho tiempo caído— y se recibe un único `restored` al recuperarse. Una sesión que se abre mientras OpenClaw está caído no llega a `ready`: el health check inicial falla y se cierra con 1011 (ver §1), así que `unavailable` no se envía nunca para el orquestador. `GET /ready` responde 503 mientras OpenClaw esté caído, sea cual sea el tiempo transcurrido. |
| `transcriber` | Una sesión STT por sesión de audio (con el proveedor `fogar`, una conexión a fogar-transcriber); reconecta en segundo plano, también cuando el transcriptor cierra la conexión limpiamente (1000/1001, p. ej. al reiniciarse): recibes `reconnecting` y luego `restored`. Si el transcriptor deja de leer el audio (proceso colgado, o su máquina caída con la conexión aún abierta), el gateway corta esa conexión a los 5 s de no poder entregarle audio y reconecta igual (`reconnecting`; con el proveedor `fogar` la frase en curso se da por perdida y se avisa con `phrase_interrupted`): tus `cancel`, `send` y `end` se atienden como mucho con ese retraso, nunca se quedan sin respuesta. `unavailable` significa que no hay transcripción automática — puedes seguir enviando texto con `send`. Puede llegar desde el arranque mismo de la sesión (ver nota en §1) o en cualquier punto intermedio. Nunca cierra la sesión. |
| `tts` | Sin conexión persistente — TTS se reconstruye en cada turno por diseño. `reconnecting`/`unavailable` aquí significa "el último intento falló y el siguiente turno con audio puede fallar también hasta que pase el backoff" — no hay un reintento en segundo plano independiente del propio turno. Los `token` de texto siguen llegando igual; solo falta el audio. Si el TTS se cae a mitad de una respuesta, el audio que ya llegó sigue sonando y el resto de esa respuesta va solo en texto, sin `status` (el siguiente turno con audio lo detecta al conectar). |

Los mensajes de estado también pueden incluir `code` y `message` con detalles adicionales. `message` es siempre un texto fijo del gateway (hoy, el mismo `code`), nunca el texto que mande el proveedor de voz:

```json
{
  "type": "status",
  "service": "transcriber",
  "state": "degraded",
  "code": "buffer_full",
  "message": "buffer_full"
}
```

---

## 9. Mensajes de error

Los errores llegan independientemente de `output_mode`:

```json
{
  "type": "error",
  "code": "TURN_ERROR",
  "message": "orchestrator_unavailable",
  "fatal": false,
  "turn_id": "t-2"
}
```

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `code` | string | Código de error (ver tabla abajo) |
| `message` | string | Descripción legible |
| `fatal` | bool | Si `true`, el gateway cierra la sesión inmediatamente después |
| `turn_id` | string | Presente solo si el error está ligado a un turno concreto |

### Códigos de error

> **Nota de precisión:** en la implementación actual, los únicos códigos que el gateway envía de verdad como mensaje `{"type":"error",...}` son `TURN_ERROR` (fallo dentro de un turno ya en marcha) e `INVALID_MESSAGE` (mensaje del cliente mal formado), ambos con `fatal: false`. Los fallos de handshake (`client_key` inválida, agente inexistente, orquestador no disponible al arrancar) **no** se comunican con un mensaje `error` previo — el gateway cierra directamente el WebSocket con el código y `reason` de la tabla de la §1. La tabla siguiente documenta la intención original del formato `code`/`fatal`; trátala como referencia de forma, no como lista exhaustiva de códigos que verás en producción hoy.

| Código | Fatal | Cuándo ocurre |
|--------|-------|---------------|
| `TURN_ERROR` | false | Fallo en un turno concreto (orquestador caído/reconectando durante una sesión activa, **o** el turno se cuelga sin progreso — `message: "turn_timeout"`, ver §13); la sesión continúa |
| `INVALID_MESSAGE` | false | Frame de texto del cliente que no es un objeto JSON: texto plano (`hola`), `42`, `[1]`, `"s"`, `true`, `null`; o `send` con `text` que no es cadena (#215). Vale en cualquier `input_mode`: desde 5.0 el texto plano ya no es un prompt (#408), los prompts van en `{"type":"send","text":...}`. La sesión continúa; el mensaje se ignora |
| `AUTH_FAILED` | true | Documentado para `client_key` inválida o inactiva — en la práctica se señaliza cerrando el WS con 4010, sin este mensaje previo |
| `AGENT_NOT_FOUND` | true | Documentado para agente inexistente — en la práctica se señaliza cerrando el WS con 4021 (o 4020 si no está permitido), sin este mensaje previo |
| `ORCHESTRATOR_UNAVAILABLE` | true | Documentado para orquestador no disponible al iniciar — en la práctica se señaliza cerrando el WS con 1011, sin este mensaje previo |
| `TTS_UNAVAILABLE` | false | Documentado para TTS caído — en la práctica ver `status: {tts, unavailable}` (§8), no este código |
| `TRANSCRIBER_UNAVAILABLE` | false | Documentado para Transcriber caído — en la práctica ver `status: {transcriber, unavailable}` (§8), no este código |
| `INTERNAL_ERROR` | true/false | Reservado para error inesperado; no confirmado en el código actual |

#### Valores de `message` de `TURN_ERROR` (estables — #228)

El `message` de un `TURN_ERROR` es siempre uno de estos códigos fijos, nunca el texto de una excepción ni un texto interno de OpenClaw (que se queda en el servidor, y ni siquiera en sus logs). Los mismos códigos llegan a los llamadores de `/v1/chat/completions` (`{"error": "<código>"}` con 502, o `error.message` dentro del flujo SSE). Cualquier otro fallo interno inesperado en `/v1` llega como `internal_error`.

| `message` | Cuándo |
|---|---|
| `orchestrator_unavailable` | El orquestador (OpenClaw) no está conectado o está reconectando al empezar el turno |
| `orchestrator_send_failed` | El gateway no pudo escribir la petición del turno al orquestador (conexión rota) |
| `orchestrator_error` | OpenClaw rechazó el turno (`res` con `ok:false`), el agente falló después de aceptarlo sin una causa de las de abajo, o falló algo interno en el camino hacia él |
| `orchestrator_auth_failed` | El agente falló porque OpenClaw no pudo autenticarse con el proveedor del modelo (clave caducada, inválida o sin permiso). Lo arregla quien administra OpenClaw, no reintentar |
| `orchestrator_model_unavailable` | El modelo del agente no existe o no está disponible, y no quedó ningún modelo de respaldo |
| `orchestrator_rate_limited` | El proveedor del modelo limitó las peticiones, está sobrecargado o sin saldo/cuota. Puede funcionar más tarde |
| `orchestrator_timeout` | El modelo o el agente tardó demasiado y OpenClaw cortó el turno (distinto de `turn_timeout`, que corta el gateway) |
| `orchestrator_agent_unavailable` | El agente de la conversación ya no está disponible para este dispositivo (lo sacaron de su lista de agentes permitidos). El gateway lo decide antes de enviar nada a OpenClaw |
| `orchestrator_disconnected` | La conexión con el orquestador se cerró con el turno en vuelo |
| `turn_timeout` | El orquestador no mandó nada durante `TURN_TIMEOUT_S` (§13); también en un push (§10) |
| `turn_overflow` | El gateway no pudo procesar los eventos del turno al ritmo de llegada (§13). Desde 5.0 ya no se envía (trátalo como `turn_timeout` si lo ves) |
| `turn_in_progress` | Ya no se envía desde 5.0: un `send` con un turno en curso espera en la cola (§4) y una petición de `/v1` con otra en curso espera a que termine (antes, HTTP 409) |
| `turn_cancelled` | Solo `/v1` (HTTP 502, nuevo en 5.0): otro aparato paró su turno (`cancel`) o el gateway se apagaba antes de terminar la respuesta |

**Desde 5.0 (#397)** los cinco códigos `orchestrator_auth_failed`, `orchestrator_model_unavailable`, `orchestrator_rate_limited`, `orchestrator_timeout` y `orchestrator_agent_unavailable` sustituyen a `orchestrator_error` cuando el gateway sabe la causa: los cuatro primeros salen solo de campos estructurales de OpenClaw (nunca de su texto), el último de la política de agentes del propio gateway. Cualquier otro código que no conozcas trátalo como `orchestrator_error`. Un push cuyo agente falla también recibe su `TURN_ERROR` antes del `turn_end` (`reason: "error"`).

**Cambio de contrato (#228):** antes, `message` podía ser `send failed: <texto de la excepción>`, `not connected`, el `error` crudo de OpenClaw (p. ej. `{'code': ..., 'message': ...}`) o el `str(e)` de una excepción interna. Si tu cliente comparaba esos textos, usa los códigos de la tabla.

Si `fatal: true`, no envíes más mensajes — el servidor cerrará la conexión WS inmediatamente.

### Cierres por conversación (M19)

Los fallos de conversación no llegan como `error`: el gateway cierra el WebSocket con uno de estos códigos (41xx, §13). Ninguno invalida la `client_key`.

| Código | `reason` | Cuándo | Qué hacer |
|---|---|---|---|
| 4100 | `conversation not found` | En el handshake: el id de `conversation` no existe o es de otro cliente | Olvida ese id; conecta sin `conversation` (la del día) o con `"new"` |
| 4101 | `conversation agent mismatch` | En el handshake: `agent` no es el de la conversación pedida | No mandes `agent` al retomar una conversación |
| 4102 | `conversation opened elsewhere` | **Retirado en la 5.0, no se reutiliza.** Una conversación puede estar abierta en varias sesiones a la vez (§14): abrirla en otra ya no cierra ninguna | — (si lo recibes, es de un gateway anterior a la 5.0) |

> **Incompatibilidad de protocolo — 5.0**
> El 4102 (`conversation opened elsewhere`) desaparece: abrir una conversación en otra sesión (otra pestaña, otro aparato, una reconexión que gana la carrera al socket viejo) ya no cierra la anterior. Las dos siguen abiertas, cada una con su par de cada turno (§5, §14). Un cliente que no reconectaba solo tras un 4102 puede quitar ese caso; uno que abría cada pestaña en su propia conversación para no echarse puede dejar de hacerlo.

---

## 10. Turns iniciados por el agente (push)

OpenClaw puede iniciar turnos proactivamente sin que el usuario haya enviado nada (notificaciones, recordatorios, alertas). El gateway los entrega al cliente con el mismo protocolo que un turno normal:

```json
{ "type": "turn_start", "turn_id": "t-3", "turn_seq": 3, "conversation": "0b9d6c1e-…", "origin": "push" }
{ "type": "token", "turn_id": "t-3", "text": "Recuerda que tienes una reunión en 5 minutos." }
// Si audio en output_mode: [0xA1][0x00][0x03][PCM16...]
{ "type": "turn_end", "turn_id": "t-3", "reason": "done" }
```

`origin: "push"` lo distingue de un turno tuyo, y `conversation` dice de qué conversación es: desde la 5.0 un push llega a todas las sesiones que tienen abierta su conversación, cada una con su par, y su audio suena en una sola (§14). **Temporal (hasta el dispositivo de avisos, 5.0 PR 2c):** un push de una conversación que ninguna sesión tiene abierta (un recordatorio de ayer, con la tele ya en la de hoy) no llega a nadie; antes llegaba a la sesión más reciente de su cliente, y en la 2c lo recogerá el dispositivo de avisos.

Un push que llega mientras suena una respuesta tuya se entrega después del `turn_end` de esa respuesta, nunca encima (los pushes siguientes esperan en orden).

Nunca hay dos push abiertos a la vez: si mientras uno está abierto llega otro de una conversación distinta (un recordatorio de ayer mientras habla uno de hoy), el segundo se descarta entero y no te llega; el abierto sigue intacto hasta su `turn_end`. Los push que lleguen después de ese `turn_end` se abren normalmente.

Al revés, hablar corta el push: si el usuario hace barge-in, envía un `send` o un `cancel` mientras un push está abierto, el gateway para su audio, te envía su `turn_end` y, si el push es de tu misma sesión de agente, lo aborta en OpenClaw. Solo después arranca el turno del usuario, así que nunca hay dos turnos abiertos a la vez (#202). Lo que quedara de ese push no llega.

Un push tiene el mismo tope que un turno normal: si OpenClaw lo abre y deja de mandar nada durante `TURN_TIMEOUT_S` (sin su fin), el gateway lo cierra con `{"type":"error","code":"TURN_ERROR","message":"turn_timeout","fatal":false,"turn_id":...}` y su `turn_end`, y lo aborta en OpenClaw si es de tu sesión de agente (#286). El siguiente push abre su propio `turn_start`.

El cliente no necesita distinguirlos de los turnos normales — el `turn_id` y `turn_seq` siguen la misma secuencia (si quiere, `origin` lo dice). Si `tool_calls_enabled` está activo, también pueden llegar mensajes `tool_call` (§5) intercalados; con `"status"` en `output_mode`, `turn_progress` (§5).

> **`push_enabled` (por defecto `True`, configurable por admin):** si tu `client_key` tiene este flag desactivado, no recibirás **ningún** mensaje de un turno iniciado por el agente — ni `turn_start`, ni `token`, ni audio, ni `tool_call` — para esa sesión. No hay ninguna señal explícita de que se haya suprimido un push; es indistinguible de que OpenClaw simplemente no haya iniciado ninguno. Si tu integración depende de notificaciones proactivas, confirma con el admin que `push_enabled=True` para tu cliente. Un `send` tuyo aborta en OpenClaw el push que estuviera en curso en tu sesión de agente aunque no lo recibas, para que tu respuesta no se mezcle con él.

> **Garantía de un único par por respuesta (issue #84):** cuando el agente hace tool use o razonamiento multi-paso, OpenClaw puede emitir varios eventos internos de inicio/fin para una sola respuesta LLM. El gateway los colapsa siempre en exactamente **un** `turn_start`/`turn_end` de cara al cliente — nunca verás duplicados. Si tu cliente implementó algún workaround para deduplicar `turn_end` repetidos (grace period, etc.) porque llegaban 2-3 veces por turno, ya no hace falta; puedes simplificarlo o quitarlo con seguridad.

> **Coordinación con turnos normales (issue #112):** si ya hay un turno normal en curso (uno que
> tú mismo iniciaste con `send`), ningún evento `agent` interno de OpenClaw para esa misma sesión
> abrirá un `turn_start` de push superpuesto al tuyo — el contenido de ese turno (tokens,
> `tool_call`) te sigue llegando con normalidad dentro del turno que ya tenías abierto. Si un
> turno de push ya estaba en curso *antes* de que empezaras el tuyo, tu `send` lo corta: su
> `turn_end` llega antes del `turn_start` de tu turno.

---

## 11. Modos de operación

### Chat de texto puro

Los prompts van siempre en mensajes `send`; un frame que no es JSON recibe `INVALID_MESSAGE` (§4, §9).

```json
// Handshake
{ "client_key": "...", "protocol": 5, "input_mode": "text", "output_mode": ["text"] }

// Enviar pregunta
{ "type": "send", "text": "¿Qué tiempo hace en Madrid?" }

// Recibir respuesta
{ "type": "turn_start", "turn_id": "t-1", "turn_seq": 1 }
{ "type": "token", "turn_id": "t-1", "text": "En Madrid " }
{ "type": "token", "turn_id": "t-1", "text": "hace sol." }
{ "type": "turn_end", "turn_id": "t-1" }
```

### Asistente de voz completo (cliente de voz de Mac y Android, app móvil; ESP32 planificado)

```json
// Handshake
{ "client_key": "...", "protocol": 5, "input_mode": "audio", "output_mode": ["audio", "text", "status"] }
```

Flujo: envía PCM Float32 → recibe parciales → envía `end` → recibe `transcription` → envías `send` → recibes `turn_start` + `token` + frames binarios + `turn_end`.

### Voz de entrada, texto de salida

```json
// Handshake
{ "client_key": "...", "protocol": 5, "input_mode": "audio", "output_mode": ["text"] }
```

El transcriptor funciona normalmente pero no se activa TTS. Útil para clientes que quieren transcripción + tokens de texto sin reproducción de audio.

### Texto de entrada, audio de salida

```json
// Handshake
{ "client_key": "...", "protocol": 5, "input_mode": "text", "output_mode": ["text", "audio"] }
```

El cliente escribe; el gateway sintetiza audio con el texto de la respuesta.

---

## 12. Referencia completa de mensajes

### Cliente → Gateway

| Mensaje | Formato | Cuándo |
|---------|---------|--------|
| Handshake | `{"client_key":"...","protocol":5,"input_mode":"...","output_mode":[...],"agent":"...","conversation":"...","capabilities":[...]}` | Primer mensaje, obligatorio |
| Fin de audio | `{"type":"end"}` | `input_mode="audio"` — usuario termina de hablar |
| Enviar texto | `{"type":"send","text":"...","interrupt":true?}` | Envía texto al orquestador: con un turno en curso espera en la cola; `interrupt: true` corta antes el turno en curso (§4) |
| Quitar un mensaje | `{"type":"withdraw","message_id":"..."}` | Quita un mensaje `pending` de la conversación, de quien sea (§4). Nuevo en 5.0 |
| Cancelar turno | `{"type":"cancel"}` | Para el turno en curso de la conversación, lo iniciara quien lo iniciara (o el push que suena, §10), sin iniciar uno nuevo |
| Frame de audio | bytes PCM Float32 16 kHz sin header | `input_mode="audio"` |
| Otro `type` (p. ej. `{"type":"keepalive"}`) | `{"type":"..."}` | Se ignora, pero cuenta como actividad para `IDLE_TIMEOUT_S` (§13) |

### Gateway → Cliente

#### Mensajes JSON

| Tipo | Formato | Cuándo |
|------|---------|--------|
| `ready` | `{"type":"ready","protocol":5,"session_id":"...","agent":"...","conversation":{"id":"...","agent":"..."},"input_mode":"...","output_mode":[...],"requested_capabilities":{...},"live_capabilities":{...}}` | Tras handshake exitoso; puede ir precedido de `status` |
| `turn_start` | `{"type":"turn_start","turn_id":"t-N","turn_seq":N mod 65536,"conversation":"..."\|null,"origin":"user"\|"push","by":{"device_id":"...","name":"...","self":bool}\|null}` | Inicio de cada turno |
| `message` | `{"type":"message","id":"...","conversation":"...","text":"...","from":{"source":"device","device_id":"...","name":"...","self":bool},"state":"pending"\|"delivered"\|"withdrawn"}` | Cada mensaje de usuario aceptado de la conversación y cada cambio de su estado, a todas las sesiones (§5). Nuevo en 5.0 |
| `token` | `{"type":"token","turn_id":"t-N","text":"..."}` | `"text"` en `output_mode` — tokens en streaming |
| `turn_end` | `{"type":"turn_end","turn_id":"t-N","reason":"done"\|"error"\|"cancelled"\|"interrupted"\|"replaced"\|"timeout"\|"closed"}` | Fin de cada turno (§5) |
| `tool_call` | `{"type":"tool_call","turn_id":"t-N","phase":"start"\|"result","name":"...","tool_call_id":"...","args":{...}\|null,"result":"..."\|null,"is_error":bool\|null}` | Solo si `tool_calls_enabled=True` para el cliente — uso de herramientas del agente |
| `turn_progress` | `{"type":"turn_progress","turn_id":"t-N","phase":"preparing"\|"thinking"\|"tool","tool":"..."\|null}` | Solo con `"status"` en `output_mode`: en qué está el turno mientras no llega texto (§5). Nuevo en #254 |
| `transcription_partial` | `{"type":"transcription_partial","text":"..."}` | `input_mode="audio"` — parciales en tiempo real |
| `transcription` | `{"type":"transcription","text":"..."}` | `input_mode="audio"` — transcripción final, espera `send`. Si el transcriptor se reconecta a mitad de frase (o la frase entera cae mientras se reabre tras la anterior), esa final no llega y en su lugar llega `status` `transcriber` `degraded` con `code: "phrase_interrupted"` (§8) |
| `status` | `{"type":"status","service":"...","state":"..."}` | Cambio de estado de un microservicio |
| `error` | `{"type":"error","code":"...","message":"...","fatal":bool,"turn_id":"..."}` | Error (turno o sesión) |
| `pipeline_event` | `{"type":"pipeline_event","stage":"...","elapsed_ms":N,"turn":N}` | Solo con `"status"` en `output_mode`: cada etapa del pipeline con los ms desde la etapa anterior (§15). Diagnóstico; no lleva texto |

#### Frames binarios de audio

```
[0xA1][turn_seq uint16 BE][PCM16 24kHz LE mono]
```

Llegan entrelazados con los mensajes JSON. Identifica audio por el magic byte `0xA1` en el primer byte.

---

## 13. Timeouts y cierre de sesión

**Nuevo — issue #115 (v1.17.0).** El gateway acota cuatro esperas que antes eran indefinidas. Tres son visibles en el wire; la cuarta (hoy `SESSION_TEARDOWN_S`, #221) es puramente interna. **Nuevo — issue #130 (S3a).** Se añade una quinta, `PUSH_TTS_DRAIN_TIMEOUT_S`, que en el wire solo se nota como audio de un turno de push que se corta. **Nuevo — issue #130 (S2).** Se añade una sexta, `CLIENT_SEND_TIMEOUT_S` (última fila): si tu socket drena por debajo del ritmo al que el gateway escribe (un cliente parado, o uno que lee demasiado despacio), el gateway te cierra con el código **1013** (`client too slow`). Un cliente que lee a un ritmo razonable no se ve afectado: en la práctica solo ocurre si tu socket se queda parado o si hay más datos en vuelo que los buffers del sistema (varios MB, más de ~2 minutos de audio TTS enviado de golpe).

| Deadline | Valor por defecto | Qué provoca | Cómo lo ves |
|---|---|---|---|
| `HANDSHAKE_TIMEOUT_S` | 5s | No mandas el JSON de handshake a tiempo | Cierre WS 4900 (ver §1) |
| `TURN_TIMEOUT_S` | 120s, **idle-reset** | El orquestador deja de mandar nada durante un turno ya en marcha, normal o push (cuelgue real, no duración total) | `{"type":"error","code":"TURN_ERROR","message":"turn_timeout","fatal":false,"turn_id":"..."}` (ver §9) — la sesión sigue viva, solo ese turno se aborta |
| `TURN_QUEUE_MAX` | 1000 eventos | Eventos del orquestador que la conversación retiene mientras espera el ack de un `send` o a que se cierre el turno abierto; es un fallo interno raro, no depende de tu lectura del socket | Desde 5.0, nada: lo que pase del tope se descarta (el gateway lo registra) y el turno sigue. Antes, `TURN_ERROR` `turn_overflow` |
| `IDLE_TIMEOUT_S` | 300s (5 min) | No mandas **ningún** mensaje (ni audio ni texto) durante ese tiempo | Cierre WS con código 1000 (`session ended`), **sin ningún mensaje de error o aviso previo** |
| `silence_timeout_s` (por cliente, default 2 s) | ~2 s con el default | Mientras envías audio, el transcriptor no devuelve nada durante más de `silence_timeout_s`. No cuenta con un turno en curso ni sin audio entrante | Un solo `status` `transcriber` `degraded` por tramo de silencio; **la sesión sigue abierta** (no se cierra nunca por silencio). El aviso se rearma tras una transcripción nueva, un turno, una pausa en el audio o una reconexión del transcriptor |
| `SESSION_REVALIDATE_INTERVAL_S` | 10s | Tu cliente se borra, se desactiva o se le rota la `client_key` con la sesión abierta; o su `allowed_agents` deja de incluir el agente de la conversación | Cierre WS 4200 (4020 si es el agente) en la siguiente revalidación; antes recibes el `turn_end` (`closed`) del turno en curso, que sigue en el canal (§14) |
| `SESSION_TEARDOWN_S` | 5s | Interno — tope de cada paso cuando el gateway cierra una sesión (cerrar tu copia del turno en curso, cerrar servicios, mandarte los últimos mensajes). Sustituye a `SHUTDOWN_DRAIN_S`, que ya no se acepta | Ninguno directo. Un turno en curso **no se espera**: recibes su `turn_end` (`closed`) si el socket sigue abierto y el turno sigue en el canal de la conversación (§14; ver «Cierre de la sesión por el gateway» abajo) |
| `PUSH_TTS_DRAIN_TIMEOUT_S` | 30s, **sin progreso** | El TTS de un turno de push (iniciado por el agente) lleva 30s sin producir audio tras el `end` del agente (se reinicia con cada trozo, así que un push largo sonando a ritmo no se corta) | El audio restante de ese push se corta, el TTS se cierra y el `turn_end` se envía igualmente — la sesión sigue viva. Con el mismo tope se acotan las escrituras de texto al TTS de push y su `end()`: si vencen, el resto de ese push llega solo como `token` de texto, sin más audio, y el `turn_end` se envía igualmente. En los turnos normales el mismo tope acota cada operación con el TTS, y la espera del audio cuenta como tiempo **sin progreso** (se reinicia con cada chunk): si vence, el turno sigue solo con texto y también recibe su `turn_end` |
| `CLIENT_SEND_TIMEOUT_S` | 10s, **por envío** | El gateway no consigue escribirte un mensaje durante ese tiempo porque tu socket **drena por debajo del ritmo al que el gateway escribe**: o has dejado de leer, o lees muy por debajo del caudal de salida (el audio TTS PCM16 a 24 kHz son 48 KB/s). Es un umbral de caudal de drenado, del orden de unos pocos KB/s (teoría) a unas pocas decenas de KB/s (medidas de laboratorio en macOS), según kernel y tasa de producción, y **solo cuando hay más datos en vuelo que los buffers del sistema** (varios MB); con respuestas de tamaño normal y un cliente que lee, no se activa; un cliente que lee a un ritmo razonable no se corta | Cierre WS con código **1013** (`client too slow`) (mejor esfuerzo) si reanudas la lectura en los 2 s siguientes; si no, o si el close frame se pierde, la conexión se corta sin close frame (1006). Los mensajes pendientes se descartan; reconecta y abre una sesión nueva |

### Códigos de cierre

> **Incompatibilidad de protocolo — M19**
> Los cierres por motivo de aplicación tienen cada uno su código 4xxx, y **el código es el contrato**: el `reason` queda como texto corto en inglés para logs y personas (≤ 123 bytes) y puede cambiar; no lo compares. Antes casi todos eran 1008 y el cliente tenía que distinguirlos por el texto del `reason` (en español y en inglés). Migración, de lo que antes era 1008 + texto a lo que es ahora:
>
> | Antes | Ahora |
> |---|---|
> | 1008 `"Handshake invalido. Se esperaba JSON de configuración."` | 4000 |
> | 1008 `"Handshake invalido. Se esperaba texto."` | 4001 |
> | 1008 `"Handshake too large"` | 4002 |
> | 1008 `"Handshake timeout"` | 4900 |
> | 1008 `"Clave de cliente invalida o inactiva."` | 4010 |
> | 1008 `"Agent '{name}' not permitted for this client."` | 4020 (el `reason` ya no lleva el nombre) |
> | 1008 `"Agent '{name}' not available."` | 4021 (ídem) |
> | 1008 `"conversation not found"` | 4100 |
> | 1008 `"conversation agent mismatch"` | 4101 |
> | 1008 `"conversation opened elsewhere"` | 4102 (retirado en la 5.0: ya no se cierra) |
> | 1008 `"revoked"` | 4200 |
> | 1011 con `reason` en español (`"Problema estableciendo…"`, `"Servicio crítico…"`) | 1011, mismo código, `reason` en inglés |
> | 1000 tras un fallo interno con la sesión abierta | 1011 `internal error` (antes no se distinguía de la inactividad) |
>
> **La regla del cliente va por rango**, para que un código nuevo no te pille: **4000–4899, no reconectes solo** (díselo al usuario); **4900–4999, reconecta con espera**. Un 4xxx que no conozcas sigue esa regla.

Los códigos estándar se quedan donde el significado es de transporte y el estándar ya lo cubre (fin normal, fallo del servidor, «vuelve a intentarlo»); cada motivo de aplicación tiene el suyo, agrupado: **40xx** handshake y credencial, **41xx** conversación, **42xx** sesión (los tres, 4000–4899: no se reconecta solo) y **49xx** fallos pasajeros de aplicación (4900–4999: se reconecta con espera). Ningún cierre del gateway usa ya 1008. La tabla vive en `src/fogar_gateway/models/close_codes.py` y un test comprueba que coincide con esta.

«Reconectar» dice qué hacer: **con espera** (solo, con backoff creciente: 1 s, 2 s, 4 s… con tope), **al usarlo** (no es un fallo y no hace falta backoff: reabre la sesión cuando la necesites; si tu cliente solo habla cuando el usuario escribe o habla, al hacerlo; si vive de escuchar *pushes*, enseguida, ver `IDLE_TIMEOUT_S` abajo), o **no, hay que arreglar algo** (la clave, el handshake o la configuración: con la misma petición el resultado será el mismo).

| Código | `reason` | Cuándo | Reconectar |
|---|---|---|---|
| 1000 | `session ended` | Fin normal: inactividad (`IDLE_TIMEOUT_S`) | Al usarlo |
| 1011 | `session setup failed` | Handshake: falló un servicio interno al montar la sesión (o al abrir la conversación). También con la sesión abierta si la configuración del cliente en la BD es ilegible (`allowed_agents` corrupto, solo editándola a mano) | Con espera |
| 1011 | `orchestrator unavailable` | Handshake: OpenClaw no responde al health check (antes llega `status` `orchestrator` `unavailable`) | Con espera |
| 1011 | `internal error` | La sesión termina por un fallo interno del gateway (antes o después del `ready`) | Con espera |
| 1013 | `client too slow` | No lees al ritmo al que el gateway escribe (`CLIENT_SEND_TIMEOUT_S`); si no reanudas la lectura en 2 s, verás 1006 | Con espera |
| 4000 | `invalid handshake` | JSON inválido, campos incorrectos o sin `client_key` | No, hay que arreglar algo (bug del cliente) |
| 4001 | `handshake must be a text frame` | El primer mensaje es binario | No, hay que arreglar algo (bug del cliente) |
| 4002 | `handshake too large` | El handshake supera `HANDSHAKE_MAX_BYTES` | No, hay que arreglar algo (bug del cliente) |
| 4003 | `protocol version not supported` | El handshake no trae `protocol: 5` | No, hay que arreglar algo: actualiza el cliente a la versión 5 |
| 4010 | `invalid or inactive credential` | `client_key` no válida, o el cliente está desactivado o retirado | No, hay que arreglar algo: vuelve a emparejar (§0) |
| 4020 | `agent not permitted` | El agente no está en `allowed_agents` del cliente, en el handshake o, con la sesión abierta, porque el administrador lo quitó (`SESSION_REVALIDATE_INTERVAL_S`; antes recibes el `turn_end` (`closed`) del turno abierto). La clave sigue valiendo | No, hay que arreglar algo: otro agente, o que el administrador lo permita |
| 4021 | `agent not available` | El agente pedido no existe en OpenClaw | No, hay que arreglar algo: otro agente |
| 4100 | `conversation not found` | La conversación pedida no existe o no es tuya | No, hay que arreglar algo: olvida ese id; sin `conversation` o con `"new"` |
| 4101 | `conversation agent mismatch` | `agent` distinto del de la conversación pedida | No, hay que arreglar algo: no mandes `agent` al retomar |
| 4102 | — | **4102 — retirado en la 5.0, no se reutiliza** (era `conversation opened elsewhere`: desde la 5.0 una conversación puede estar abierta en varias sesiones, §14) | — |
| 4200 | `credential revoked` | Con la sesión abierta, tu cliente se borró, se desactivó o se rotó su clave (`SESSION_REVALIDATE_INTERVAL_S`); antes recibes el `turn_end` (`closed`) del turno abierto | No, hay que arreglar algo: vuelve a emparejar (§0) |
| 4900 | `handshake timeout` | No mandas el handshake en `HANDSHAKE_TIMEOUT_S` (una red lenta, un cliente que se atascó al conectar) | Con espera |

Cierres que no manda el gateway pero verás: **1006** (la conexión se cortó sin close frame: la red, o el abort tras un 1013), **1009** (un frame de más de 1 MiB: lo cierra el servidor WebSocket antes de que llegue al gateway; es un bug del cliente) y **1012** (el gateway se reinicia o se para). Ante 1006 o 1012, reconecta con espera.

### Cuidado con el ping de la librería WebSocket de tu cliente

Muchas librerías WebSocket (p. ej. `websockets` de Python: `ping_interval=20 s` y `ping_timeout=20 s` por defecto) cierran la conexión si no reciben el `pong` a tiempo. Con el pacing del audio (§6) el gateway solo tiene ~2 s de audio por delante, así que el `pong` ya no queda detrás de minutos de audio. Aun así, si tu cliente deja de leer el socket mientras reproduce, la librería puede quedarse sin `pong` y **cerrar ella misma la conexión** (1011/1006) sin que el gateway haga nada.

Qué hacer en el cliente: desactivar o ampliar el ping de la librería (`ping_interval=None` o un `ping_timeout` mayor), o leer del socket en una tarea aparte de la reproducción y poner el audio en una cola propia. Y, en cualquier caso, **reconectar ante cualquier cierre inesperado** (1006, 1011, 1012, 1013, EOF) abriendo una sesión nueva con handshake completo; ante un 4xxx, no (ver «Códigos de cierre»).

### `TURN_TIMEOUT_S` es "idle-reset", no un techo total

El reloj se reinicia con cada evento que llega del orquestador (cada token, cada evento de herramienta). Un turno con una respuesta larga pero activa (tool-use multi-paso, por ejemplo) nunca se corta por esto — solo se corta si el orquestador deja de mandar absolutamente nada durante 120s seguidos. No necesitas ningún cambio de cliente para esto: ya manejas `TURN_ERROR` (§9), y `turn_timeout` es simplemente un valor más de `message` dentro de ese mismo mecanismo.

### `IDLE_TIMEOUT_S` — la que sí te afecta si mantienes conexiones abiertas

Esta es la novedad más relevante para clientes de larga duración (dispositivos siempre-conectados, sesiones que solo reciben *pushes* del agente sin que el usuario hable):

- El contador se basa **solo en mensajes entrantes del cliente** (audio o texto) — nada que el gateway te mande a ti cuenta como actividad.
- **Se pausa mientras haya un turno o un push en curso**: si el orquestador está respondiendo activamente o hay un turno iniciado por el agente abierto, el cierre por idle no se dispara aunque hayan pasado los 5 minutos — el contador solo corre cuando de verdad no hay nada pasando en ninguna dirección.
- Si se dispara, el gateway cierra el WebSocket con 1000 (`session ended`, cierre normal) — **no** manda un `error` ni un `status` avisando antes. Lo distingues por el código: no es un fallo, y la tabla de «Códigos de cierre» dice reconectar **al usarlo**, sin backoff.
- No hay campo en `ready` que indique este valor — si tu cliente necesita conocerlo para decidir cuándo mandar un keep-alive, tiene que estar hardcodeado o configurado del lado del cliente (no se expone por protocolo todavía).

**Qué implica para tu cliente** («al usarlo» según lo que haga tu cliente):
- Si tu cliente solo habla cuando el usuario escribe o habla (el chat del panel), no reconectes tras el 1000: reabre la sesión cuando el usuario vuelva a usarlo.
- Si vive de escuchar *pushes* del agente sin que el usuario interactúe durante más de 5 minutos seguidos, para él «usarlo» es estar conectado: o manda algo periódicamente para resetear el contador (aunque sea un mensaje que el gateway ignore a nivel de negocio, cualquier frame válido cuenta), o reabre la sesión en cuanto recibe el 1000, sin backoff (no es un fallo).

### Cierre de la sesión por el gateway — informativo

No requiere ningún cambio de cliente si ya reconectas tras un cierre inesperado.

- **Turno en curso:** si el gateway cierra la sesión con un turno abierto, no se espera a que termine: recibes su `turn_end` (`closed`) antes del cierre si el socket sigue abierto, y el turno **sigue** en el canal de la conversación (desde la 5.0 no se cancela ni se aborta: lo ven las demás sesiones adjuntas, y una que se adjunte después lo recibe con el texto acumulado, §14). Nunca queda un `turn_start` sin su `turn_end` mientras puedas recibirlo. La inactividad (`IDLE_TIMEOUT_S`) y el silencio no cortan un turno en curso. Un push huérfano (abierto sin su fin) no la retiene: a los `TURN_TIMEOUT_S` sin progreso se cierra solo, con su `turn_end` (§10).
- **Reinicio o parada del gateway:** el servidor cierra cada WebSocket con el código **1012** (*service restart*) en cuanto empieza a apagarse; un turno en curso se corta en ese momento y su `turn_end` ya no te llega (el socket está cerrado). No hay ventana de gracia para terminar el turno. Trata el 1012 como un cierre esperado: reconecta (con una pequeña espera y reintentos, el gateway tarda unos segundos en volver) y abre una sesión nueva con handshake completo.
- **Revocación y agente retirado:** desde ese momento no te llega nada más (ni audio, ni tokens, ni avisos; tampoco lo que ya estaba en cola) salvo el `turn_end` del turno en curso (y de un push abierto), con `reason: "closed"`, y el `turn_start` de ese turno si aún no había salido (nunca un `turn_end` sin su `turn_start`; un `send` aceptado que no llegó a empezar recibe también su par, con `closed`); después, el cierre 4200 (revocada) o 4020 (el administrador quitó el agente de la sesión de `allowed_agents`). El turno no se aborta: si la conversación está abierta en otras sesiones, sigue en ellas (§14). Igual con el 1011 `session setup failed` de una sesión abierta cuya configuración en la BD resulta ilegible. Antes de M19 la revocación cerraba sin ese `turn_end`.

`SHUTDOWN_DRAIN_S` (antes 30 s) prometía esperar al turno en curso en un reinicio; en la práctica eso no ocurría (el servidor ya había cerrado el socket). Desde #221 no hacía nada y, desde 5.0, ya no se acepta: si sigue en `config.env` el gateway no arranca y `fogar-gw doctor` lo nombra; bórralo.

---

## 14. Conversaciones

**Nuevo en M19.** Cada sesión habla en una **conversación**: una sesión de OpenClaw con su historial, que se puede retomar otro día. El gateway solo guarda su índice (de qué cliente es, con qué agente, cuándo se habló); lo dicho y el título viven en OpenClaw. El administrador las ve todas en `/admin/conversations` (ver el [README](../README.md#admin-requiere-x-admin-token)).

Las reglas son las mismas para todos los clientes; el gateway no mira el tipo de cliente. Que tu cliente use una conversación del día o elija las suyas es una decisión de diseño de tu cliente:

- **Sin `conversation` en el handshake**, la sesión cae en la **conversación del día**: una por cliente, agente y día. El día empieza a las `CONVERSATION_DAY_START_HOUR` (5:00 por defecto, hora local del gateway). Se evalúa **al empezar cada turno tuyo**: una sesión que sigue abierta al pasar el corte (una tele con el socket abierto días) pasa a la conversación del día nuevo en su primer turno después, y lo verás en `turn_start.conversation`. `ready.conversation` es la del momento de conectar. Un push no cambia la conversación de la sesión.
- **Con `"conversation": "new"`**, el gateway crea una conversación nueva para tu cliente (solo si la sesión llega a `ready`: un handshake que acaba en 1011 no deja una conversación vacía); **con el `id` de una de tus conversaciones** (el que te dio `ready.conversation.id`), la retomas: OpenClaw sigue el hilo. Una sesión que eligió conversación **no cambia nunca de día** por sí sola. Solo puedes pedir conversaciones de tu propio cliente: cualquier otro id cierra con 4100 (`conversation not found`). Una conversación fija su agente (§1).
- **Varias sesiones en una conversación (5.0).** Una conversación puede estar abierta en varias sesiones a la vez (la misma del día en dos aparatos, el mismo id en dos pestañas, una reconexión antes de que caiga el socket viejo): ninguna cierra a otra (el 4102 se retiró, §9). Cada turno de la conversación, lo envíe quien lo envíe, llega a cada sesión con su propio `turn_start`/`turn_end` (su `turn_id`, su `turn_seq`); el audio, solo a la sesión que lo envió. Una sesión que entra con un turno en marcha recibe su `turn_start` y el texto acumulado en un `token` (§5). Una sesión que se va no corta el turno para las demás. Un `cancel` («Parar») desde cualquiera de las sesiones para el turno de la conversación para todas (todas reciben su `turn_end` `cancelled`). La voz no: un barge-in o una transcripción final solo cortan un turno que envió esa misma sesión, y de un push, solo su copia en esa sesión (el push sigue en las demás); un cliente de voz que oye la tele no le corta su turno. Un `send` durante un turno no lo corta: espera en la cola de OpenClaw y todas las sesiones ven su `message` `pending` (§4, §5); con `interrupt` corta el turno en curso (todas reciben su `turn_end` `interrupted`); `withdraw` quita un pendiente, sea de quien sea. Una sesión que entra recibe también los mensajes pendientes. Solo te adjuntas a conversaciones que puedes ver (abajo).
- **Push:** llega a todas las sesiones que tienen abierta su conversación (cada una con su par; el audio, en una sola). Temporal hasta la 5.0 PR 2c: un push de una conversación que ninguna sesión tiene abierta no llega a nadie (lo recogerá el dispositivo de avisos, §10).
- **Home Assistant (`/v1`)** sigue la misma regla del día para su cliente (con `Authorization: Bearer`).

> **Incompatibilidad de protocolo — M19**
> La sesión de OpenClaw ya no es una por cliente y agente para siempre: es la de la conversación. Un cliente que no manda `conversation` empieza conversación (y contexto) nuevo cada día a las `CONVERSATION_DAY_START_HOUR`. (En M19, abrir dos sesiones en la misma conversación cerraba la primera con 4102; desde la 5.0 conviven, ver abajo.)

> **Incompatibilidad de protocolo — 5.0**
> Una conversación ya no está abierta en una sola sesión: varias sesiones (del mismo cliente o de otros dispositivos que pueden verla) comparten la conversación y cada una recibe su par de cada turno. El 4102 se retiró y no se reutiliza. Un cliente que abre la conversación del día en dos sitios ya no se echa a sí mismo.

**Quién abre qué (4.0).** Una conversación es del hogar o de una persona, según el dispositivo que la crea: la de un dispositivo **personal** (con sesión) es privada de su usuario; la de uno **del hogar** (emparejado), del hogar. Desde un dispositivo del hogar se abren solo las del hogar; desde uno personal, las del hogar y las de su usuario, también las empezadas en otro de sus dispositivos. Pedir una que no se puede ver es 4100, igual que una que no existe. Un aviso (push) de una conversación privada nunca llega a un dispositivo del hogar. La web inicia sesión (`/auth/login`), entra con su token como `client_key`, crea sus conversaciones con `POST /admin/conversations` (o `"conversation": "new"`) y las retoma por su id.

---

## 15. Diagnóstico del pipeline (`pipeline_event`)

Solo con `"status"` en `output_mode`. El gateway mide cada sesión por etapas (las mismas que enseña `/admin/sessions` y que van al log a INFO) y, a un cliente que pide `"status"`, le manda cada una según ocurre:

```json
{ "type": "pipeline_event", "stage": "llm_first_token", "elapsed_ms": 2840, "turn": 3 }
```

| Campo | Descripción |
|-------|-------------|
| `stage` | La etapa (tabla siguiente) |
| `elapsed_ms` | Milisegundos (entero) desde la etapa **anterior de la sesión**, sea cual sea, no desde el inicio del turno. Para medir un tramo (fin de frase → primer audio), suma los de las etapas que lo forman |
| `turn` | El contador de turnos **del usuario** de la sesión (0 antes del primer `send`). No es el `turn_id` ni el `turn_seq`: un push no lo incrementa, y sus etapas llevan el del último turno del usuario |

No lleva texto (ni del usuario ni de la respuesta) ni los detalles que el gateway guarda para `/admin/sessions` (longitudes, voz, tiempos internos del TTS): solo la etapa y los dos números.

| `stage` | Cuándo |
|---|---|
| `session_start` | Al empezar la sesión, justo después de `ready` |
| `transcription_partial` | Cada parcial de transcripción (`input_mode: "audio"`), después de su `transcription_partial` |
| `barge_in` | Un parcial cortó el turno en curso (§7), después de su `turn_end` |
| `transcription_final` | La transcripción final, antes de su `transcription` |
| `transcription_incomplete` | El transcriptor cortó una final por saturación (`complete: false`, `reason` del transcriptor) o una reconexión cortó la frase en curso o la frase entera cayó mientras se reabría tras la anterior (`reason: "phrase_interrupted"`, la final no se entrega) |
| `llm_start` | Un turno del usuario sale hacia el orquestador (después de su `turn_start`) |
| `llm_first_token` | Llega el primer texto del orquestador, justo antes del primer `token` |
| `llm_done` | El orquestador terminó la respuesta |
| `tts_start` | Empieza la síntesis de voz del turno (con `"audio"`; también en un push) |
| `tts_first_chunk` | Primer audio del TTS, antes de su primer frame |
| `tts_done` | El TTS entregó todo el audio del turno |
| `push_tts` | Se cerró el TTS de un push (al terminar o al cortarse) |
| `session_end` | La sesión se cierra (si el socket aún puede recibirlo) |

Un push no tiene `llm_*`: solo `tts_*` y `push_tts` si hay audio. Las etapas de un cierre forzado (`client_slow`, `session_revoked`, `agent_revoked`, `session_setup_failed`) quedan en `/admin/sessions` pero no llegan: el socket ya no las puede recibir o solo deja salir el `turn_end` (§13).

Van en el mismo orden total que el resto (§5), pero desde la 5.0 su posición respecto a los mensajes de turno solo es orientativa: el turno lo pinta cada sesión por su cuenta (§14), y un `pipeline_event` puede llegar antes del `turn_end` del turno anterior (p. ej. el `llm_start` de tu turno nuevo mientras aún sale el final del anterior). Pueden llegar fuera de un turno (`session_start`, `transcription_*`, `session_end`). Son diagnóstico: la lista de etapas puede crecer y un cliente no debe depender de ellas para funcionar (para enseñar en qué está un turno, `turn_progress`, §5). Ignora las etapas que no conozcas.
