#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
Cátedra: Sistemas Distribuidos - UTN FRCU
Ejercicio 3: Comunicación Confiable con TCP - Servidor Persistente Concurrente
=============================================================================
Descripción pedagógica:
TCP (Transmission Control Protocol) es orientado a conexión, confiable,
full-duplex y garantiza entrega ordenada y libre de errores.

Flujo de primitivas en el servidor:
1. socket(AF_INET, SOCK_STREAM): Crea el socket TCP (SOCK_STREAM).
2. setsockopt(SOL_SOCKET, SO_REUSEADDR, 1): Evita el bloqueo del puerto en estado TIME_WAIT.
3. bind((ip, port)): Asocia el socket a la interfaz de red y puerto local.
4. listen(backlog): Pone al socket en modo pasivo a la espera de peticiones de conexión (SYN).
5. accept(): Bloquea hasta completar el 3-Way Handshake (SYN, SYN-ACK, ACK) con un cliente.
   Devuelve un NUEVO socket dedicado exclusivamente a ese cliente y su dirección.
6. recv() / sendall(): Transmisión y recepción confiable de bytes a través del nuevo socket.
7. shutdown() / close(): Cierre ordenado de la sesión TCP.
=============================================================================
"""

import socket
import threading
import argparse
import sys
import time
from datetime import datetime
from protocolo_tcp import enviar_mensaje, recibir_mensaje

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass


def atender_cliente(conn: socket.socket, addr: tuple, id_cliente: int):
    """
    Función ejecutada en un hilo independiente para atender a un cliente
    específico en una sesión TCP persistente.
    """
    ip_remota, puerto_remoto = addr
    print(f"\n [CONEXIÓN ESTABLECIDA] Hilo #{id_cliente} atendiendo a {ip_remota}:{puerto_remoto}")
    mensajes_procesados = 0

    try:
        # Bucle de conexión persistente: permite múltiples mensajes por sesión
        while True:
            # 1. Espera y recibe un mensaje delimitado según el protocolo de enmarcado
            mensaje = recibir_mensaje(conn)

            # Si recibir_mensaje retorna None, el cliente envió un paquete TCP FIN (cierre limpio)
            if mensaje is None:
                print(f" [CLIENTE DESCONECTADO LIMPIAMENTE] Hilo #{id_cliente} ({ip_remota}:{puerto_remoto}) cerró la sesión.")
                break

            mensajes_procesados += 1
            ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            print(f"[{ahora}] [Hilo #{id_cliente} | Msg #{mensajes_procesados}] Recibido: \"{mensaje}\"")

            # Procesamiento de la solicitud
            respuesta = f"ACK_TCP_SERVER: Procesado '{mensaje.upper()}' | MsgSeq: {mensajes_procesados}"

            # 2. Envío de respuesta delimitada
            enviar_mensaje(conn, respuesta)
            print(f"[{ahora}] [Hilo #{id_cliente} | Msg #{mensajes_procesados}] Respuesta enviada.")

    except ConnectionResetError:
        print(f" [DESCONEXIÓN ABRUPTA] El cliente {ip_remota}:{puerto_remoto} (Hilo #{id_cliente}) reinició o abortó la conexión (RST).")
    except ConnectionAbortedError:
        print(f" [CONEXIÓN ABORTADA] Conexión cerrada localmente para {ip_remota}:{puerto_remoto}.")
    except Exception as e:
        print(f" [ERROR EN HILO #{id_cliente}] {e}", file=sys.stderr)
    finally:
        # 3. Cierre del socket dedicado a este cliente
        conn.close()
        print(f" [SOCKET CLIENTE LIBERADO] Sesión finalizada para {ip_remota}:{puerto_remoto}. Total mensajes: {mensajes_procesados}\n")


def iniciar_servidor_tcp(ip: str, puerto: int, backlog: int = 10):
    """
    Inicializa el servidor TCP pasivo y acepta conexiones entrantes en hilos.
    """
    # 1. Creación del socket TCP
    # AF_INET = IPv4 | SOCK_STREAM = Flujo confiable de bytes TCP
    servidor_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # 2. Opción SO_REUSEADDR para poder reiniciar el servidor de inmediato
    servidor_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        # 3. Enlace (bind)
        servidor_sock.bind((ip, puerto))

        # 4. Puesta en modo pasivo de escucha (listen)
        servidor_sock.listen(backlog)

        print("=" * 75)
        print(f" [SERVIDOR TCP CONFIABLE] Activo y escuchando en {ip}:{puerto}")
        print(f" [CONFIG] Backlog de conexiones pendientes: {backlog}")
        print(" [MODO] Conexiones persistentes y concurrentes activadas (Multihilo)")
        print(" Presione Ctrl+C para apagar el servidor limpiamente.")
        print("=" * 75)

        contador_clientes = 0

        while True:
            # 5. accept(): Primitiva bloqueante que espera un cliente entrante
            # y retorna un socket nuevo 'conn' dedicado y la dirección 'addr'.
            conn, addr = servidor_sock.accept()
            contador_clientes += 1

            # Lanzar un hilo independiente para permitir atender a múltiples clientes
            # de forma simultánea sin que una sesión persistente bloquee a los demás.
            hilo_cliente = threading.Thread(
                target=atender_cliente,
                args=(conn, addr, contador_clientes),
                daemon=True  # Permite que el programa principal finalice aunque haya hilos activos
            )
            hilo_cliente.start()

    except KeyboardInterrupt:
        print("\n [SERVIDOR TCP] Apagado solicitado por teclado (Ctrl+C)...")
    except Exception as e:
        print(f"\n [ERROR FATAL DEL SERVIDOR] {e}", file=sys.stderr)
    finally:
        # 6. Cierre del socket maestro de escucha
        servidor_sock.close()
        print(" [SERVIDOR TCP] Socket de escucha cerrado. Servidor detenido.")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Servidor TCP Confiable y Persistente (Sistemas Distribuidos - UTN FRCU)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-i", "--ip", default="0.0.0.0", help="Dirección IP de escucha")
    parser.add_argument("-p", "--puerto", type=int, default=7000, help="Puerto TCP de escucha")
    parser.add_argument("-b", "--backlog", type=int, default=10, help="Cola de espera para conexiones pendientes")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    iniciar_servidor_tcp(args.ip, args.puerto, args.backlog)
