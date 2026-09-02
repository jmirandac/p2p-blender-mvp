# Chat P2P WebRTC de terminal

Este proyecto conecta dos terminales mediante un `RTCDataChannel` de WebRTC.
[`aiortc`](https://github.com/aiortc/aiortc) se encarga de ICE/STUN, la apertura de
la ruta P2P, DTLS y SCTP; ya no hay un cliente STUN ni un protocolo UDP propios.

El flujo tiene dos partes:

1. un servidor WebSocket empareja dos clientes que usan el mismo código de sala y
   retransmite la oferta y respuesta SDP;
2. una vez negociado el canal, los mensajes del chat viajan directamente y cifrados
   entre ambos peers mediante WebRTC.

El servidor de señalización ve los identificadores de los peers y las descripciones
SDP, pero **no recibe ni retransmite mensajes del chat**.

## Instalación

Requiere Python 3.10 o posterior.

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

## Prueba rápida en una sola máquina

Abre tres terminales desde esta carpeta.

Terminal 1:

```bash
python3 signaling_server.py
```

Terminal 2:

```bash
python3 peer.py --room demo --name alice
```

Terminal 3:

```bash
python3 peer.py --room demo --name bob
```

Usa `/salir` para cerrar el chat.

## Prueba entre dos redes distintas

Ejecuta el servidor de señalización en una máquina accesible desde Internet y
permite tráfico TCP entrante al puerto 9000:

```bash
python3 signaling_server.py --host 0.0.0.0 --port 9000
```

Después, en cada peer:

```bash
python3 peer.py --server HOST_PUBLICO --port 9000 \
  --room un-codigo-compartido --name alice
```

El puerto 9000 solo transporta la señalización WebSocket. WebRTC selecciona sus
propios puertos UDP mediante ICE. El servidor STUN puede cambiarse con
`--stun-server`; para desactivarlo en una red local, usa `--stun-server ""`.

En producción, la señalización debería servirse con TLS y los clientes usar
`--secure` para conectarse mediante WSS.

## Pruebas automatizadas

```bash
python3 -m unittest discover -v
```

## Límites del MVP

- Solo admite dos peers por sala.
- No autentica usuarios ni protege el código de sala.
- Incluye STUN pero no configura TURN. Algunos NAT simétricos, firewalls o redes
  corporativas necesitarán un servidor TURN añadido a `RTCConfiguration`.
- El canal P2P está cifrado por DTLS, pero el WebSocket de señalización solo usa TLS
  cuando se despliega detrás de un endpoint WSS.
