# Estado de la evolución del signaling server

- Estado general: `Completada`
- Plan aprobado: `Sí`
- Última actualización: `2026-09-04`

## Estados

- `Pendiente`: todavía no iniciado.
- `En curso`: trabajo activo.
- `Bloqueada`: no puede continuar; el motivo se anota en evidencia.
- `Completada`: implementado y verificado.

## Hitos

| ID | Hito | Estado | Dependencia | Criterio de finalización | Evidencia |
|---|---|---|---|---|---|
| H1 | Modelo de peers y sesiones | Completada | Plan aprobado | Estados, transiciones y estructuras implementados y probados | Registro `Peer`, sesiones y limpieza idempotente implementados en `p2pchat/signaling.py` |
| H2 | Registro, listado y desconexión | Completada | H1 | Registro por nombre, ID del servidor, listado y cierre voluntario operativos | Cubierto por `test_registers_unique_ids_lists_and_retains_disconnected_peer` |
| H3 | Heartbeat y limpieza | Completada | H1 | Caídas detectadas y sesiones limpiadas en todos los estados | Ping/pong configurable y prueba de desconexión inesperada |
| H4 | Invitaciones y consentimiento | Completada | H2, H3 | Aceptación, rechazo, busy y timeout funcionan correctamente | Pruebas de rechazo, expiración, destinos inválidos y reserva simultánea |
| H5 | Negociación y ciclo del chat | Completada | H4 | SDP, `chat-ready`, timeout y retorno a espera implementados | Pruebas de secuencia SDP, ready de ambos extremos, cierre y timeout |
| H6 | Cliente terminal interactivo | Completada | H2–H5 | CLI maneja comandos, eventos asíncronos y varios chats por conexión | `SignalingClient` y prueba de dos chats WebRTC consecutivos |
| H7 | Pruebas integrales y concurrencia | Completada | H1–H6 | Suite unitaria e integración completa en verde | 10 pruebas en verde, incluida concurrencia y dos chats WebRTC consecutivos |
| H8 | Documentación y validación final | Completada | H7 | README actualizado, revisión final superada y estado general completado | README y ayuda CLI verificados; `git diff --check` y compilación correctos |
| H9 | Eliminar espera fija de recolección ICE | Completada | H8 | STUN conserva candidato público sin bloquear cinco segundos la negociación | Oferta reducida de 5,009 s a 0,506 s conservando `srflx`; flujo aceptación–chat en 1,051 s; 11 pruebas en verde |

## Registro de cambios de alcance

- 2026-09-04: plan aprobado para implementación sin cambios de alcance.
- 2026-09-04: se añade H9 tras detectar una espera de cinco segundos durante la recolección ICE/STUN.
