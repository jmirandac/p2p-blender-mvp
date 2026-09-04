# Chat P2P WebRTC de terminal

Este proyecto conecta terminales mediante un `RTCDataChannel` de WebRTC. Un
servidor WebSocket mantiene el registro de peers conectados, permite descubrirlos,
media el consentimiento entre ellos y retransmite las ofertas y respuestas SDP.
Los mensajes del chat viajan directamente y cifrados entre los dos peers.

Una misma conexión con el servidor puede utilizarse para varios chats sucesivos.
Al terminar un chat, ambos peers vuelven a estar disponibles.

## Instalación

Requiere Python 3.10 o posterior.

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

## Prueba rápida

Abre tres terminales desde esta carpeta.

Servidor:

```bash
python3 signaling_server.py
```

### Ejecución con Docker

Construye la imagen del servidor de señalización:

```bash
docker build -t p2p-signaling-server .
```

Inicia el contenedor y publica el puerto 9000 en el host:

```bash
docker run --rm --name p2p-signaling -p 9000:9000 p2p-signaling-server
```

El servidor queda disponible en `ws://localhost:9000`. Los argumentos del
servidor se pueden configurar mediante variables de entorno:

| Variable | Parámetro | Valor por defecto |
| --- | --- | ---: |
| `SIGNALING_HOST` | `--host` | `0.0.0.0` |
| `SIGNALING_PORT` | `--port` | `9000` |
| `SIGNALING_HEARTBEAT_INTERVAL` | `--heartbeat-interval` | `10` |
| `SIGNALING_HEARTBEAT_TIMEOUT` | `--heartbeat-timeout` | `20` |
| `SIGNALING_INVITE_TIMEOUT` | `--invite-timeout` | `15` |
| `SIGNALING_NEGOTIATION_TIMEOUT` | `--negotiation-timeout` | `30` |

Por ejemplo:

```bash
docker run --rm --name p2p-signaling -p 9000:9000 \
  -e SIGNALING_HEARTBEAT_INTERVAL=15 \
  -e SIGNALING_HEARTBEAT_TIMEOUT=30 \
  p2p-signaling-server
```

Si una variable no está definida, se conserva el valor actual por defecto. Los
argumentos de línea de comandos también siguen disponibles y tienen prioridad
sobre las variables de entorno:

```bash
docker run --rm -p 9000:9000 p2p-signaling-server \
  --host 0.0.0.0 --port 9000 \
  --heartbeat-interval 15 --heartbeat-timeout 30
```

Primer peer:

```bash
python3 peer.py --name alice
```

Segundo peer:

```bash
python3 peer.py --name bob
```

Cada cliente muestra el ID único asignado por el servidor. Desde cualquiera de
ellos, ejecuta `/peers`, copia el ID del destino y solicita el chat:

```text
/conectar peer_ID_DEL_DESTINO
```

El destino debe responder `/aceptar` o `/rechazar`. Después de aceptar y completar
la negociación, cualquier texto que no comience por `/` se envía por el canal P2P.

## Comandos del cliente

- `/peers`: muestra los peers conectados y si están disponibles u ocupados.
- `/conectar ID`: solicita un chat con el peer indicado.
- `/aceptar`: acepta la invitación recibida.
- `/rechazar`: rechaza la invitación recibida.
- `/salir`: termina el chat actual y vuelve al estado de espera.
- `/desconectar`: abandona el servidor y cierra el cliente.

Los nombres visibles pueden repetirse; el ID asignado por el servidor es la
identidad inequívoca de cada conexión.

## Configuración

El servidor acepta las siguientes opciones:

```bash
python3 signaling_server.py \
  --host 0.0.0.0 \
  --port 9000 \
  --heartbeat-interval 10 \
  --heartbeat-timeout 20 \
  --invite-timeout 15 \
  --negotiation-timeout 30
```

- El heartbeat WebSocket detecta conexiones que dejan de responder.
- Una invitación sin respuesta se rechaza al vencer su timeout.
- Una negociación WebRTC que no abre ambos extremos del canal también expira.

El cliente limita por defecto a 0,5 segundos la espera bloqueante de candidatos
STUN. Esto evita que interfaces VPN o virtuales sin respuesta introduzcan la pausa
fija de cinco segundos de `aioice`, conservando los candidatos host y los
candidatos STUN que respondan dentro de ese intervalo:

```bash
python3 peer.py --name alice --ice-gather-timeout 0.5
```

El valor puede aumentarse en redes con mucha latencia. Esta opción no es el
timeout global de negociación del signaling server.

Para acceder a un servidor remoto:

```bash
python3 peer.py --server HOST_PUBLICO --port 9000 --name alice
```

El puerto 9000 solo transporta registro y señalización. WebRTC selecciona sus
propios puertos mediante ICE. El servidor STUN puede cambiarse con
`--stun-server`; para desactivarlo en una red local, usa `--stun-server ""`.

En producción, la señalización debería servirse con TLS y los clientes usar
`--secure` para conectarse mediante WSS.

## Estados del protocolo

Un peer conectado puede estar en `waiting`, `inviting`, `deciding`, `negotiating`
o `chatting`. Solo `waiting` está disponible para una nueva invitación. Si un peer
se desconecta durante una invitación, negociación o chat, el servidor libera al
otro participante y lo devuelve a `waiting`.

La especificación completa está en [`doc/PLAN.md`](doc/PLAN.md) y el avance de la
implementación en [`doc/STATUS.md`](doc/STATUS.md).

## Pruebas automatizadas

```bash
python3 -m unittest discover -v
```

La suite cubre el protocolo del servidor, los timeouts, la concurrencia y dos chats
WebRTC sucesivos sin reconectar al signaling server.

## Límites actuales

- El registro es volátil y local a un único proceso.
- No hay autenticación ni recuperación de identidad.
- No se mantienen las antiguas salas ni su protocolo.
- No se encolan invitaciones y cada peer solo puede participar en una sesión.
- Incluye STUN pero no configura TURN. Algunos NAT simétricos, firewalls o redes
  corporativas necesitarán un servidor TURN.
- El canal P2P está cifrado por DTLS, pero el WebSocket solo usa TLS al desplegarse
  detrás de un endpoint WSS.
