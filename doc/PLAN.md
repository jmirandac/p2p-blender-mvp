# Evolución del signaling server a registro interactivo de peers

## Objetivo

Sustituir el emparejamiento automático por salas por un protocolo WebSocket
persistente. Cada cliente se registra con un nombre, recibe un ID único de sesión
y permanece conectado al servidor mientras lista peers, gestiona invitaciones y
establece uno o varios chats P2P sucesivos.

El servidor mantiene en memoria el registro y el estado de cada peer, coordina el
consentimiento, retransmite la señalización WebRTC y devuelve a los participantes
al estado de espera cuando termina el chat.

## Modelo de estado y comportamiento

- Estado de conexión: `connected` o `disconnected`.
- Estado de actividad: `waiting`, `inviting`, `deciding`, `negotiating` o
  `chatting`.
- Solo un peer `connected/waiting` puede iniciar o recibir una invitación. El
  resto se considera `busy`.
- El servidor genera un `peer_id` opaco por conexión. Los nombres pueden
  repetirse y deben contener entre 1 y 64 caracteres seguros.
- Los peers desconectados se conservan en memoria hasta reiniciar el proceso,
  sin referencias al WebSocket y con sus tiempos de conexión y desconexión.
- El listado excluye al solicitante y devuelve los peers conectados con `id`,
  `name` y `availability: waiting|busy`.
- Una invitación reserva atómicamente a ambos peers. Un rechazo o timeout los
  devuelve a `waiting`; una aceptación los lleva a `negotiating`.
- El solicitante inicia WebRTC. Ambos peers deben enviar `chat-ready` antes de
  pasar a `chatting`.
- Al terminar o fallar un chat, los participantes aún conectados vuelven a
  `waiting`.
- Si cae un WebSocket, ese peer pasa a `disconnected` y el otro participante es
  notificado y liberado.
- Toda limpieza es idempotente para tolerar cierres simultáneos y eventos tardíos.

## Protocolo

### Registro y descubrimiento

- `register {name}` → `registered {peer: {id, name}}`.
- `list-peers {request_id}` → `peer-list {request_id, peers}`.
- `disconnect` → `disconnected`, seguido del cierre del WebSocket.

### Invitaciones

- `connect-request {request_id, target_id}`.
- `connect-pending {request_id, session_id}` para el solicitante.
- `connection-request {session_id, peer: {id, name}, expires_in}` para el destino.
- `connection-response {session_id, accepted}`.
- Los resultados negativos distinguen `rejected`, `timeout`, `busy`,
  `unavailable` y `self`.
- La aceptación produce `matched {session_id, peer, initiator}` para ambos peers.

### Negociación y chat

- `description {session_id, description}` retransmite ofertas y respuestas SDP.
- `chat-ready {session_id}` confirma localmente el canal WebRTC.
- `chat-started {session_id}` se emite cuando ambos están listos.
- `chat-end {session_id, reason}` solicita finalizar la sesión.
- `session-ended {session_id, reason}` notifica el cierre y retorno a espera.
- Los errores usan `error {code, message, request_id?}` y no cierran la conexión
  cuando son recuperables.

## Configuración y cliente

- Timeout de heartbeat: 20 segundos; intervalo de ping: 10 segundos.
- Timeout de invitación: 15 segundos.
- Timeout de negociación: 30 segundos.
- El heartbeat permanece activo durante todos los estados.
- La CLI elimina `--room`, muestra el ID asignado y ofrece `/peers`,
  `/conectar ID`, `/aceptar`, `/rechazar`, `/salir` y `/desconectar`.
- `/salir` cierra solamente el chat; `/desconectar` abandona el signaling server.
- El receptor WebSocket funciona permanentemente mientras se espera entrada de
  terminal o se usa el canal P2P.

## Implementación

- Reemplazar los mapas de salas por un registro de peers, un índice por WebSocket
  y sesiones temporales con participantes, fase, confirmaciones y timeouts.
- Proteger las transiciones con el lock del servidor y enviar fuera del lock con
  un lock individual por peer.
- Centralizar la finalización de invitaciones, negociaciones y chats.
- Autorizar las descripciones SDP por sesión, participante, rol y secuencia.
- Reestructurar el cliente como una máquina de estados, manteniendo `WebRTCChat`
  como responsable exclusivo del canal P2P.
- Actualizar pruebas y README al nuevo protocolo.

## Pruebas y aceptación

- Registro, IDs únicos, nombres duplicados, validación y desconexión.
- Listado y disponibilidad.
- Invitaciones aceptadas, rechazadas, expiradas, simultáneas, cruzadas, al
  propio peer, a IDs inexistentes y a peers ocupados.
- Autorización y secuencia SDP.
- Confirmación de ambos `chat-ready` y timeout de negociación.
- Cierre normal, fallo WebRTC, caída WebSocket y heartbeat fallido.
- Segundo chat sin reconectar tras `/salir`.
- Integración completa con dos clientes `aiortc`.

## Fuera de alcance

- Persistencia entre reinicios, autenticación o recuperación de identidad.
- Compatibilidad con el protocolo anterior basado en rooms.
- Colas de invitaciones o múltiples chats simultáneos por peer.
- TLS/WSS, TURN, autorización, métricas y despliegue distribuido.
- Transporte de mensajes de chat a través del servidor.
