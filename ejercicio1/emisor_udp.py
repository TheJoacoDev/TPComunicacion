#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
Cátedra: Sistemas Distribuidos - UTN FRCU
Ejercicio 1: Comunicación por Datagramas UDP - Emisor (Cliente)
=============================================================================
Descripción pedagógica:
El emisor crea un socket UDP y envía datagramas directamente hacia la dirección IP
y puerto de destino. A diferencia de TCP, no existe un handshake previo (SYN, SYN-ACK, ACK)
ni se establece un canal persistente. Si la máquina receptora está apagada o
el puerto cerrado, sendto() no arroja error inmediato a menos que el sistema operativo
reciba un paquete ICMP "Port Unreachable" posterior.

Primitivas de sockets utilizadas:
- socket(AF_INET, SOCK_DGRAM): Crea el descriptor de socket UDP.
- settimeout(segundos): Establece un tiempo límite para operaciones de socket.
- sendto(bytes, (ip_destino, puerto_destino)): Envía un datagrama completo.
- close(): Cierra el socket.
=============================================================================
"""

import socket
import argparse
import sys
import time
from datetime import datetime


if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass


def enviar_mensaje(sock: socket.socket, ip_destino: str, puerto_destino: int, mensaje: str):
    """
    Codifica en UTF-8 y envía un mensaje mediante el socket UDP provisto.
    """
    datos = mensaje.encode("utf-8")
    t_inicio = time.perf_counter()

    # sendto(): Primitiva central de UDP.
    # Envía el búfer de bytes al par (IP, Puerto) indicado.
    bytes_enviados = sock.sendto(datos, (ip_destino, puerto_destino))
    t_fin = time.perf_counter()

    ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"[{ahora}] -> Datagrama enviado con exito:")
    print(f"  |-- Destino: {ip_destino}:{puerto_destino}")
    print(f"  |-- Bytes transmitidos: {bytes_enviados} bytes")
    print(f"  |-- Tiempo de invocacion sendto: {(t_fin - t_inicio) * 1000:.3f} ms")
    print(f"  \\-- Contenido: \"{mensaje}\"")


def iniciar_emisor(ip_destino: str, puerto_destino: int, mensaje: str = None, timeout: float = 3.0):
    """
    Inicializa el socket del emisor y gestiona el envío puntual o interactivo.
    """
    # 1. Creación del socket UDP
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # 2. Configuración de timeout:
    # Si bien sendto suele ser no bloqueante con búfers libres, el timeout protege
    # si se reutilizara el socket para esperar confirmación o respuestas ICMP.
    if timeout:
        sock.settimeout(timeout)

    print("=" * 70)
    print(" [EMISOR UDP] Inicializado")
    print(f" [DESTINO CONFIGURADO] {ip_destino}:{puerto_destino}")
    print(f" [TIMEOUT DE SOCKET] {timeout} s")
    print("=" * 70)

    try:
        # Caso A: Se especificó un mensaje único por parámetro CLI
        if mensaje is not None:
            enviar_mensaje(sock, ip_destino, puerto_destino, mensaje)
        # Caso B: Modo interactivo en consola
        else:
            print(" Modo interactivo activado. Escriba un mensaje y presione ENTER.")
            print(" Ingrese 'salir' o presione Ctrl+C para terminar.\n")
            while True:
                linea = input(" > Ingrese mensaje a transmitir: ").strip()
                if not linea:
                    continue
                if linea.lower() in ("salir", "exit", "quit"):
                    print(" Saliendo del modo interactivo...")
                    break
                enviar_mensaje(sock, ip_destino, puerto_destino, linea)
                print("-" * 50)

    except socket.timeout:
        print(f" [TIMEOUT EXCEDIDO] Operación de socket superó los {timeout} segundos.", file=sys.stderr)
    except socket.error as e:
        print(f" [ERROR DE RED / SOCKET] {e}", file=sys.stderr)
    except KeyboardInterrupt:
        print("\n [EMISOR UDP] Envío cancelado por el usuario (Ctrl+C).")
    finally:
        # 3. Liberación del socket
        sock.close()
        print(" [EMISOR UDP] Socket liberado.")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Emisor de datagramas UDP (Sistemas Distribuidos - UTN FRCU)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-d", "--destino",
        type=str,
        default="127.0.0.1",
        help="Dirección IP de destino (ej. '127.0.0.1' local o IP de la otra máquina en la LAN)"
    )
    parser.add_argument(
        "-p", "--puerto",
        type=int,
        default=5000,
        help="Puerto UDP de destino"
    )
    parser.add_argument(
        "-m", "--mensaje",
        type=str,
        default=None,
        help="Mensaje de texto a enviar (si se omite, se abre la consola interactiva)"
    )
    parser.add_argument(
        "-t", "--timeout",
        type=float,
        default=3.0,
        help="Timeout en segundos para operaciones del socket"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    iniciar_emisor(args.destino, args.puerto, args.mensaje, args.timeout)
