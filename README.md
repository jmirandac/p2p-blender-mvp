# Chat P2P de terminal (MVP)

Este proyecto demuestra las tres fases de una conexión P2P:

1. cada cliente consulta un servidor STUN público de Google para conocer su IP y puerto públicos;
2. un servidor de señalización empareja dos clientes que usan el mismo código de sala e intercambia sus endpoints;
3. ambos clientes hacen *UDP hole punching* y, cuando se encuentran, todos los mensajes viajan directamente entre ellos.

El servidor de señalización **no recibe ni retransmite mensajes del chat**. No hay dependencias externas: basta Python 3.10 o posterior.

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

Aunque esta prueba se ejecuta localmente, sigue haciendo una consulta STUN real. Los clientes probarán tanto el endpoint público como el local y elegirán el primero que funcione.

## Prueba entre dos redes distintas

Ejecuta el servidor de señalización en una máquina accesible desde Internet y permite tráfico TCP entrante al puerto 9000:

```bash
python3 signaling_server.py --host 0.0.0.0 --port 9000
```

Después, en cada peer:

```bash
python3 peer.py --server HOST_PUBLICO --port 9000 --room un-codigo-compartido --name alice
```

El puerto 9000 solo se usa para señalización TCP. El chat utiliza un puerto UDP asignado automáticamente. Puedes fijarlo con `--udp-port 50000`, aunque normalmente no es necesario.

## Pruebas automatizadas

```bash
python3 -m unittest discover -v
```

## Límites deliberados del MVP

- Solo admite dos peers por sala.
- Usa IPv4 y UDP; no implementa TCP P2P.
- No cifra ni autentica mensajes y el código de sala no es un secreto robusto.
- No incluye TURN/relay. Algunos NAT simétricos, firewalls o redes corporativas impedirán una conexión directa aunque STUN funcione.
- UDP no garantiza entrega ni orden. Para producción convendría usar WebRTC/ICE (STUN + TURN), autenticación y cifrado.

