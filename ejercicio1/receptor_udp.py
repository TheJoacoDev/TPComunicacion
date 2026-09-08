#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
Cátedra: Sistemas Distribuidos - UTN FRCU
Ejercicio 1: Comunicación por Datagramas UDP - Receptor (Servidor)
=============================================================================
Descripción pedagógica:
UDP (User Datagram Protocol) es un protocolo no orientado a conexión (Connectionless)
que no garantiza entrega, orden ni control de flujo. Cada invocación a sendto()
genera un datagrama independiente en la red que se recibe de forma indivisible
mediante recvfrom().

Primitivas de sockets utilizadas:
- socket(AF_INET, SOCK_DGRAM): Crea un endpoint de comunicación UDP/IPv4.
- bind((ip, port)): Asocia el socket local a una dirección IP y puerto específico
  para poder recibir datagramas dirigidos a dicha interfaz.
- recvfrom(bufsize): Bloquea la ejecución esperando un datagrama entrante.
  Retorna (datos, (ip_origen, puerto_origen)).
- close(): Libera los recursos del socket y el puerto asociado en el SO.
=============================================================================
"""

import socket
import argparse
import sys
from datetime import datetime


if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass


def iniciar_receptor(ip: str, puerto: int, buffer_size: int, timeout: float = None, max_mensajes: int = None):
    """
    Inicia un receptor UDP en el host y puerto especificados.

    :param ip: Dirección IP en la que escuchará el socket ('0.0.0.0' para todas las interfaces).
    :param puerto: Puerto UDP local de escucha.
    :param buffer_size: Tamaño máximo en bytes asignado para leer cada datagrama.
    :param timeout: Tiempo en segundos tras el cual el socket arroja socket.timeout si no recibe datos.
    :param max_mensajes: Cantidad máxima opcional de datagramas a recibir antes de salir.
    """
    # 1. Creación del socket UDP:
    # AF_INET = Protocolo IPv4
    # SOCK_DGRAM = Protocolo de Datagramas (UDP)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Permitir reutilización rápida de la dirección si el socket quedó en estado de espera
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        # 2. Enlace (bind) del socket a la interfaz de red y puerto:
        # Al usar '0.0.0.0', el socket escuchará en todas las tarjetas de red (Ethernet, Wi-Fi, Loopback).
        sock.bind((ip, puerto))
        print("=" * 70, flush=True)
        print(f" [RECEPTOR UDP] Servidor activo escuchando en {ip}:{puerto}", flush=True)
        print(f" [CONFIG] Tamaño de buffer: {buffer_size} bytes", flush=True)
        if timeout:
            sock.settimeout(timeout)
            print(f" [CONFIG] Timeout de inactividad: {timeout} segundos", flush=True)
        else:
            print(" [CONFIG] Modo bloqueante (sin timeout de socket)", flush=True)
        if max_mensajes:
            print(f" [CONFIG] Limite de recepción: {max_mensajes} mensaje(s)", flush=True)
        print(" [ESTADO] Esperando datagramas... Presione Ctrl+C para finalizar.", flush=True)
        print("=" * 70, flush=True)

        contador_mensajes = 0

        while True:
            try:
                # 3. Espera bloqueante de un datagrama:
                # recvfrom recibe como parámetro el buffer_size.
                # NOTA PEDAGÓGICA: En UDP, si el mensaje entrante es mayor que buffer_size,
                # los bytes excedentes se descartan silenciosamente por el sistema operativo
                # o generan una excepción/error de truncamiento (en Windows WSAEMSGSIZE).
                datos, (ip_emisor, puerto_emisor) = sock.recvfrom(buffer_size)
                contador_mensajes += 1

                ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                mensaje_texto = datos.decode("utf-8", errors="replace")

                print(f"\n[{ahora}] -> Datagrama #{contador_mensajes} recibido:", flush=True)
                print(f"  |-- Origen: {ip_emisor}:{puerto_emisor}", flush=True)
                print(f"  |-- Longitud: {len(datos)} bytes (Buffer disp: {buffer_size} bytes)", flush=True)
                print(f"  \\-- Contenido: \"{mensaje_texto}\"", flush=True)

                if max_mensajes is not None and contador_mensajes >= max_mensajes:
                    print(f" [RECEPTOR UDP] Se alcanzo el limite de {max_mensajes} mensajes. Finalizando receptor...", flush=True)
                    break

            except socket.timeout:
                print(f" [TIMEOUT] No se recibieron datagramas en los ultimos {timeout} segundos.", flush=True)
            except socket.error as e:
                # En Windows, si el buffer es muy chico puede lanzar WSAEMSGSIZE (error 10040)
                print(f" [ADVERTENCIA/ERROR DE SOCKET] {e}", flush=True)

    except KeyboardInterrupt:
        print("\n [RECEPTOR UDP] Interrupción por teclado (Ctrl+C). Finalizando receptor...")
    except Exception as e:
        print(f"\n [ERROR FATAL] Error al enlazar o ejecutar el receptor: {e}", file=sys.stderr)
    finally:
        # 4. Cierre del socket y liberación de recursos
        sock.close()
        print(" [RECEPTOR UDP] Socket cerrado correctamente. Fin de ejecución.")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Receptor de datagramas UDP (Sistemas Distribuidos - UTN FRCU)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-i", "--ip",
        type=str,
        default="0.0.0.0",
        help="Dirección IP de escucha ('0.0.0.0' para escuchar en todas las interfaces de red)"
    )
    parser.add_argument(
        "-p", "--puerto",
        type=int,
        default=5000,
        help="Puerto UDP de escucha"
    )
    parser.add_argument(
        "-b", "--buffer",
        type=int,
        default=1024,
        help="Tamaño de buffer de lectura en bytes"
    )
    parser.add_argument(
        "-t", "--timeout",
        type=float,
        default=None,
        help="Timeout de socket opcional en segundos (None = bloqueante indefinido)"
    )
    parser.add_argument(
        "-n", "--max-mensajes",
        type=int,
        default=None,
        help="Cantidad máxima de mensajes a recibir antes de finalizar (None = infinito)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    iniciar_receptor(args.ip, args.puerto, args.buffer, args.timeout, args.max_mensajes)
