#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
Cátedra: Sistemas Distribuidos - UTN FRCU
Ejercicio 3: Comunicación Confiable con TCP
Módulo de Enmarcado: Length-Prefixed Framing (Delimitación de Mensajes)
=============================================================================
Fundamento Pedagógico:
TCP es un protocolo de flujo continuo de bytes (Byte Stream), no orientado a
mensajes. A diferencia de UDP, TCP no preserva los límites de los mensajes:
múltiples llamadas a send() pueden fusionarse en un solo segmento de red
(coalescencia por algoritmo de Nagle) o un único mensaje puede llegar fragmentado
en múltiples llamadas a recv().

Para garantizar la entrega íntegra y ordenada a nivel de aplicación, se
implementa aquí un protocolo de enmarcado con prefijo de longitud:
  [ 4 Bytes: Longitud N (Big-Endian) ] + [ N Bytes: Carga útil UTF-8 ]
=============================================================================
"""

import socket
import struct
from typing import Optional


# Formato de la cabecera: '!I' -> Network byte order (Big-Endian), unsigned 32-bit integer (4 bytes)
HEADER_FORMAT = "!I"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


def _recibir_exactamente(sock: socket.socket, num_bytes: int) -> Optional[bytes]:
    """
    Función auxiliar que lee de forma acumulativa del socket TCP hasta obtener
    exactamente 'num_bytes' o detectar el fin de la conexión (EOF).

    :param sock: Socket TCP conectado.
    :param num_bytes: Cantidad exacta de bytes requeridos.
    :return: Bytes acumulados, o None si el par cerró la conexión ordenadamente.
    """
    buffer = bytearray()
    while len(buffer) < num_bytes:
        bytes_faltantes = num_bytes - len(buffer)
        chunk = sock.recv(bytes_faltantes)
        if not chunk:
            # Si recv retorna 0 bytes, indica que el par cerró la conexión (EOF - FIN recibido)
            if len(buffer) == 0:
                return None
            else:
                raise ConnectionResetError("Conexión interrumpida mientras se leía un mensaje incompleto.")
        buffer.extend(chunk)
    return bytes(buffer)


def enviar_mensaje(sock: socket.socket, texto: str):
    """
    Empaqueta y transmite un mensaje de texto sobre un stream TCP garantizando
    su delimitación mediante prefijo de longitud.

    :param sock: Socket TCP conectado.
    :param texto: Cadena de texto a transmitir.
    """
    payload_bytes = texto.encode("utf-8")
    longitud = len(payload_bytes)

    # 1. Empaquetar la longitud en 4 bytes (Big-Endian)
    cabecera = struct.pack(HEADER_FORMAT, longitud)

    # 2. sendall(): Primitiva de TCP que reintenta internamente el envío
    # hasta que la totalidad de los bytes del búfer hayan sido entregados
    # a la cola de transmisión del SO.
    sock.sendall(cabecera + payload_bytes)


def recibir_mensaje(sock: socket.socket) -> Optional[str]:
    """
    Lee del stream TCP exactamente un mensaje delimitado.

    :param sock: Socket TCP conectado.
    :return: Cadena de texto recibida, o None si la conexión fue cerrada limpiamente.
    """
    # 1. Leer los 4 bytes de la cabecera para conocer la longitud del mensaje
    cabecera_bytes = _recibir_exactamente(sock, HEADER_SIZE)
    if cabecera_bytes is None:
        return None  # Conexión cerrada limpiamente

    # Desempaquetar la longitud del payload
    (longitud_payload,) = struct.unpack(HEADER_FORMAT, cabecera_bytes)

    # 2. Leer exactamente la cantidad de bytes indicada por la cabecera
    payload_bytes = _recibir_exactamente(sock, longitud_payload)
    if payload_bytes is None:
        raise ConnectionResetError("Conexión cerrada inesperadamente esperando el payload.")

    return payload_bytes.decode("utf-8", errors="replace")
