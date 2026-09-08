#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
Cátedra: Sistemas Distribuidos - UTN FRCU
Ejercicio 3: Comunicación Confiable con TCP - Cliente Resiliente y Persistente
=============================================================================
Descripción pedagógica:
El cliente TCP establece un canal de flujo continuo de bytes hacia el servidor.
A diferencia de UDP, garantiza:
  1. Entrega confiable (retransmisión automática a nivel de transporte).
  2. Entrega en orden secuencial.
  3. Control de congestión y control de flujo por ventana deslizante.

Para lidiar con fallas del entorno distribuido, este cliente implementa:
  - Sesión persistente: reutiliza el mismo socket TCP para múltiples peticiones.
  - Resiliencia y auto-reconexión con Exponential Backoff ante caída del servidor.
  - Cierre limpio (Graceful Shutdown): shutdown(SHUT_WR) seguido de close().
=============================================================================
"""

import socket
import argparse
import sys
import time
from datetime import datetime
from typing import Optional
from protocolo_tcp import enviar_mensaje, recibir_mensaje

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass


class ClienteTCPResiliente:
    """
    Cliente TCP con soporte para sesiones persistentes y reconexión automática.
    """

    def __init__(self, host: str, puerto: int, max_reintentos: int = 5, backoff_inicial: float = 1.0):
        self.host = host
        self.puerto = puerto
        self.max_reintentos = max_reintentos
        self.backoff_inicial = backoff_inicial
        self.sock: Optional[socket.socket] = None

    def conectar(self) -> bool:
        """
        Intenta establecer la conexión TCP con el servidor.
        Si falla, realiza reintentos con espera exponencial (Exponential Backoff).
        """
        intento = 0
        demora = self.backoff_inicial

        while intento < self.max_reintentos:
            intento += 1
            try:
                print(f" [CONECTANDO] Intento #{intento}/{self.max_reintentos} conectando a {self.host}:{self.puerto}...")
                # 1. Crear nuevo socket TCP
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.settimeout(5.0)  # Timeout para la fase de conexión

                # 2. connect(): Inicia el Three-Way Handshake (SYN -> SYN-ACK -> ACK)
                self.sock.connect((self.host, self.puerto))
                self.sock.settimeout(None)  # Quitar timeout para sesión persistente

                print(f" [CONECTADO EXITOSAMENTE] Enlace TCP establecido con {self.host}:{self.puerto}")
                return True

            except (ConnectionRefusedError, socket.timeout, OSError) as e:
                print(f" [FALLO DE CONEXIÓN] {e}. Reintentando en {demora:.1f}s...")
                time.sleep(demora)
                demora = min(demora * 2, 10.0)  # Duplica el tiempo hasta un máximo de 10s

        print(f" [ERROR CRÍTICO] No se pudo conectar tras {self.max_reintentos} intentos.", file=sys.stderr)
        return False

    def enviar_y_recibir(self, texto: str) -> Optional[str]:
        """
        Envía un mensaje delimitado y aguarda la respuesta del servidor.
        Si detecta una desconexión abrupta, intenta reconectar automáticamente.
        """
        if self.sock is None:
            if not self.conectar():
                return None

        while True:
            try:
                t0 = time.perf_counter()
                # Envío de mensaje enmarcado
                enviar_mensaje(self.sock, texto)

                # Recepción de respuesta enmarcada
                respuesta = recibir_mensaje(self.sock)

                if respuesta is None:
                    # El servidor cerró la conexión
                    print(" [AVISO] El servidor cerró la conexión durante la lectura.")
                    self.cerrar_socket_actual()
                    print(" [RECUPERACIÓN] Intentando reconectar al servidor...")
                    if self.conectar():
                        continue
                    else:
                        return None

                rtt = (time.perf_counter() - t0) * 1000
                return f"{respuesta} (RTT: {rtt:.2f} ms)"

            except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError) as e:
                print(f"\n [DESCONEXIÓN ABRUPTA DETECTADA] {e}")
                self.cerrar_socket_actual()
                print(" [RECUPERACIÓN AUTOMÁTICA] Iniciando procedimiento de reconexión...")
                if self.conectar():
                    print(" [CONEXIÓN RESTABLECIDA] Reenviando mensaje pendiente...")
                    continue
                else:
                    return None

    def cerrar_socket_actual(self):
        """Cierra el socket actual de forma segura."""
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

    def cerrar_limpiamente(self):
        """
        Cierre elegante (Graceful Shutdown) del socket:
        1. shutdown(socket.SHUT_WR): Envía el paquete TCP FIN al servidor,
           anunciando que no se enviarán más datos desde este extremo.
        2. close(): Libera el descriptor en el SO.
        """
        if self.sock:
            print(" [CIERRE LIMPIO] Enviando FIN (shutdown SHUT_WR)...")
            try:
                self.sock.shutdown(socket.SHUT_WR)
            except Exception:
                pass
            self.cerrar_socket_actual()
            print(" [CIERRE LIMPIO] Socket cerrado. Sesión TCP finalizada con éxito.")


def iniciar_cliente_tcp(host: str, puerto: int, mensaje: str = None, max_reintentos: int = 5):
    cliente = ClienteTCPResiliente(host, puerto, max_reintentos=max_reintentos)

    print("=" * 75)
    print(f" [CLIENTE TCP CONFIABLE] Destino: {host}:{puerto}")
    print("=" * 75)

    if not cliente.conectar():
        sys.exit(1)

    try:
        if mensaje is not None:
            # Envío puntual
            resp = cliente.enviar_y_recibir(mensaje)
            print(f" [RESPUESTA DEL SERVIDOR] {resp}")
        else:
            # Modo interactivo persistente
            print("\n [MODO PERSISTENTE] Escriba mensajes para enviarlos por el mismo canal TCP.")
            print(" Ingrese 'salir' para cerrar la conexión limpiamente (Graceful Shutdown).\n")
            num_msg = 0
            while True:
                num_msg += 1
                texto = input(f" [Msg #{num_msg}] > Ingrese texto: ").strip()
                if not texto:
                    continue
                if texto.lower() in ("salir", "exit", "quit"):
                    break
                resp = cliente.enviar_y_recibir(texto)
                if resp is not None:
                    print(f" < Servidor: {resp}\n")
                else:
                    print(" [ERROR] Imposible obtener respuesta del servidor.")
                    break

    except KeyboardInterrupt:
        print("\n [CLIENTE TCP] Interrumpido por usuario (Ctrl+C).")
    finally:
        cliente.cerrar_limpiamente()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Cliente TCP Confiable y Resiliente (Sistemas Distribuidos - UTN FRCU)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-d", "--destino", default="127.0.0.1", help="Dirección IP del servidor TCP")
    parser.add_argument("-p", "--puerto", type=int, default=7000, help="Puerto TCP del servidor")
    parser.add_argument("-m", "--mensaje", type=str, default=None, help="Mensaje puntual a transmitir")
    parser.add_argument("-r", "--reintentos", type=int, default=5, help="Cantidad máxima de reintentos con backoff")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    iniciar_cliente_tcp(args.destino, args.puerto, args.mensaje, args.reintentos)
