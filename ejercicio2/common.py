#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
Cátedra: Sistemas Distribuidos - UTN FRCU
Ejercicio 2: Protocolo Petición-Respuesta Multívía
Módulo Común: Mensajes y Manejador de Sockets Desacoplados (RX / TX)
=============================================================================
Este módulo define:
1. La estructura serializable de mensajes del protocolo (JSON).
2. La clase CanalesDesacoplados que administra simultáneamente dos sockets UDP:
   - Socket RX: Exclusivo para recepción, ligado a (bind_ip, rx_port).
   - Socket TX: Exclusivo para transmisión, ligado a (bind_ip, tx_port).
=============================================================================
"""

import socket
import json
import time
import sys
from typing import Optional, Tuple, Dict, Any

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass


class TipoMensaje:
    """Tipos de mensajes contemplados en las 2, 3 y 4 vías."""
    REQUEST = "REQUEST"           # Solicitud inicial enviada por A
    ACK_REQ = "ACK_REQUEST"       # Acuse de recibo de la solicitud (utilizado en 4 vías)
    RESPONSE = "RESPONSE"         # Respuesta con el resultado del cómputo enviada por B
    ACK_RES = "ACK_RESPONSE"      # Acuse de recibo final de la respuesta enviado por A (3 y 4 vías)


class MensajeProtocolo:
    """Estructura de un paquete del protocolo Petición-Respuesta."""

    def __init__(self, tipo: str, seq_id: int, emisor: str, payload: Any = None, timestamp: float = None):
        self.tipo = tipo
        self.seq_id = seq_id
        self.emisor = emisor
        self.payload = payload
        self.timestamp = timestamp or time.time()

    def to_json_bytes(self) -> bytes:
        """Serializa la instancia a bytes UTF-8 con formato JSON."""
        datos = {
            "tipo": self.tipo,
            "seq_id": self.seq_id,
            "emisor": self.emisor,
            "payload": self.payload,
            "timestamp": self.timestamp
        }
        return json.dumps(datos, ensure_ascii=False).encode("utf-8")

    @classmethod
    def from_json_bytes(cls, datos_bytes: bytes) -> "MensajeProtocolo":
        """Deserializa bytes UTF-8 a una instancia de MensajeProtocolo."""
        datos = json.loads(datos_bytes.decode("utf-8"))
        return cls(
            tipo=datos["tipo"],
            seq_id=datos["seq_id"],
            emisor=datos["emisor"],
            payload=datos.get("payload"),
            timestamp=datos.get("timestamp")
        )

    def __str__(self) -> str:
        return f"[Tipo: {self.tipo} | ID: {self.seq_id} | De: {self.emisor} | Payload: {self.payload}]"


class CanalesDesacoplados:
    """
    Gestiona el desacople físico/lógico de recepción (RX) y transmisión (TX).
    Cumple con la regla: 'Cada proceso debe enviar y recibir por puertos distintos'.
    """

    def __init__(self, nombre_nodo: str, bind_ip: str, rx_port: int, tx_port: int, timeout_rx: float = 5.0):
        self.nombre_nodo = nombre_nodo
        self.bind_ip = bind_ip
        self.rx_port = rx_port
        self.tx_port = tx_port
        self.timeout_rx = timeout_rx

        # 1. Socket de Recepción (RX)
        self.sock_rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_rx.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock_rx.bind((self.bind_ip, self.rx_port))
        if self.timeout_rx is not None:
            self.sock_rx.settimeout(self.timeout_rx)

        # 2. Socket de Transmisión (TX)
        # Se enlaza explícitamente a tx_port para que los paquetes salientes
        # lleven garantizado el puerto de origen tx_port (verificable con Wireshark).
        self.sock_tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_tx.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock_tx.bind((self.bind_ip, self.tx_port))

    def enviar(self, mensaje: MensajeProtocolo, dest_ip: str, dest_rx_port: int) -> int:
        """
        Envía un mensaje desde el puerto TX hacia el puerto RX del destinatario.
        """
        paquete = mensaje.to_json_bytes()
        bytes_enviados = self.sock_tx.sendto(paquete, (dest_ip, dest_rx_port))
        print(f"[{self.nombre_nodo}] [TX: {self.tx_port} -> {dest_ip}:{dest_rx_port}] "
              f"Enviado {mensaje.tipo} (Seq: {mensaje.seq_id}) | {len(paquete)} bytes")
        return bytes_enviados

    def recibir(self, buffer_size: int = 4096) -> Tuple[MensajeProtocolo, Tuple[str, int]]:
        """
        Recibe un mensaje bloqueante (respetando timeout_rx) en el puerto RX.
        Retorna (MensajeProtocolo, (ip_origen, puerto_origen)).
        """
        datos_bytes, dir_origen = self.sock_rx.recvfrom(buffer_size)
        mensaje = MensajeProtocolo.from_json_bytes(datos_bytes)
        print(f"[{self.nombre_nodo}] [RX: {self.rx_port} <- {dir_origen[0]}:{dir_origen[1]}] "
              f"Recibido {mensaje.tipo} (Seq: {mensaje.seq_id})")
        return mensaje, dir_origen

    def cerrar(self):
        """Cierra de manera ordenada ambos descriptores de socket."""
        try:
            self.sock_rx.close()
        except Exception:
            pass
        try:
            self.sock_tx.close()
        except Exception:
            pass
        print(f"[{self.nombre_nodo}] Canales RX ({self.rx_port}) y TX ({self.tx_port}) cerrados.")
