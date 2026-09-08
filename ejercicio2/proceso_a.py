#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
Cátedra: Sistemas Distribuidos - UTN FRCU
Ejercicio 2: Protocolo Petición-Respuesta Multívía - Proceso A (Iniciador)
=============================================================================
Descripción pedagógica:
Proceso A actúa como el cliente/iniciador del intercambio de mensajes.
Implementa la regla de puertos desacoplados:
  - Puerto de escucha (RX): 5001 (por defecto)
  - Puerto de transmisión (TX): 5002 (por defecto)

Soporta 3 modos de intercambio:
  a) 2 vías: Request -> Response
  b) 3 vías: Request -> Response -> ACK
  c) 4 vías: Request -> ACK -> Response -> ACK
=============================================================================
"""

import argparse
import socket
import sys
import time
from common import CanalesDesacoplados, MensajeProtocolo, TipoMensaje

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass


def ejecutar_transaccion(canales: CanalesDesacoplados, dest_ip: str, dest_rx_port: int,
                         payload: str, seq_id: int, protocolo: str, timeout: float):
    """
    Ejecuta una transacción completa de petición-respuesta según el modo de vías seleccionado.
    """
    t_inicio = time.perf_counter()
    print("\n" + "=" * 75)
    print(f" [PROCESO A] Iniciando Transacción #{seq_id} | Protocolo: {protocolo} Vías")
    print(f" Petición: '{payload}'")
    print("=" * 75)

    try:
        # =====================================================================
        # PASO 1 (Común a 2, 3 y 4 vías): Envío de REQUEST
        # =====================================================================
        msg_req = MensajeProtocolo(
            tipo=TipoMensaje.REQUEST,
            seq_id=seq_id,
            emisor="Proceso_A",
            payload=payload
        )
        print(f" [Vía 1] Enviando REQUEST a Proceso B ({dest_ip}:{dest_rx_port})...")
        canales.enviar(msg_req, dest_ip, dest_rx_port)

        # =====================================================================
        # PROTOCOLO DE 2 VÍAS (Request -> Response)
        # =====================================================================
        if protocolo == "2":
            print(" [Vía 2] Esperando RESPONSE de Proceso B en puerto RX...")
            canales.sock_rx.settimeout(timeout)
            msg_resp, _ = canales.recibir()

            if msg_resp.tipo == TipoMensaje.RESPONSE and msg_resp.seq_id == seq_id:
                rtt = (time.perf_counter() - t_inicio) * 1000
                print(f" [ÉXITO 2-VÍAS] Respuesta recibida: {msg_resp.payload}")
                print(f" RTT total: {rtt:.2f} ms")
            else:
                print(f" [ALERTA] Mensaje inesperado recibido: {msg_resp}")

        # =====================================================================
        # PROTOCOLO DE 3 VÍAS (Request -> Response -> ACK)
        # =====================================================================
        elif protocolo == "3":
            print(" [Vía 2] Esperando RESPONSE de Proceso B en puerto RX...")
            canales.sock_rx.settimeout(timeout)
            msg_resp, _ = canales.recibir()

            if msg_resp.tipo == TipoMensaje.RESPONSE and msg_resp.seq_id == seq_id:
                print(f" [RESPUESTA RECIBIDA] {msg_resp.payload}")

                # Vía 3: A envía confirmación ACK a B
                print(" [Vía 3] Enviando confirmación final (ACK_RESPONSE) a B...")
                msg_ack = MensajeProtocolo(
                    tipo=TipoMensaje.ACK_RES,
                    seq_id=seq_id,
                    emisor="Proceso_A",
                    payload={"status": "CONFIRMED", "received_at": time.time()}
                )
                canales.enviar(msg_ack, dest_ip, dest_rx_port)
                rtt = (time.perf_counter() - t_inicio) * 1000
                print(f" [ÉXITO 3-VÍAS] Protocolo de 3 vías completado satisfactoriamente. RTT: {rtt:.2f} ms")
            else:
                print(f" [ALERTA] Mensaje inesperado recibido: {msg_resp}")

        # =====================================================================
        # PROTOCOLO DE 4 VÍAS (Request -> ACK -> Response -> ACK)
        # =====================================================================
        elif protocolo == "4":
            # Vía 2: Esperar acuse de recibo de la petición (ACK_REQUEST)
            print(" [Vía 2] Esperando acuse de recibo de solicitud (ACK_REQUEST) en puerto RX...")
            canales.sock_rx.settimeout(timeout)
            msg_ack_req, _ = canales.recibir()

            if msg_ack_req.tipo == TipoMensaje.ACK_REQ and msg_ack_req.seq_id == seq_id:
                print(f" [ACK RECIBIDO] Servidor confirmó recepción de la tarea: {msg_ack_req.payload}")
            else:
                print(f" [ALERTA] Se esperaba ACK_REQUEST pero se recibió: {msg_ack_req}")

            # Vía 3: Esperar el resultado final del cómputo (RESPONSE)
            print(" [Vía 3] Esperando resultado final (RESPONSE) en puerto RX...")
            # Otorgamos un margen extra de tiempo para el cómputo en 4 vías
            canales.sock_rx.settimeout(timeout + 5.0)
            msg_resp, _ = canales.recibir()

            if msg_resp.tipo == TipoMensaje.RESPONSE and msg_resp.seq_id == seq_id:
                print(f" [RESPUESTA DEFINITIVA RECIBIDA] {msg_resp.payload}")

                # Vía 4: A envía acuse de recibo final (ACK_RESPONSE)
                print(" [Vía 4] Enviando confirmación final (ACK_RESPONSE) a Proceso B...")
                msg_ack_res = MensajeProtocolo(
                    tipo=TipoMensaje.ACK_RES,
                    seq_id=seq_id,
                    emisor="Proceso_A",
                    payload={"status": "TASK_DONE_CONFIRMED"}
                )
                canales.enviar(msg_ack_res, dest_ip, dest_rx_port)
                rtt = (time.perf_counter() - t_inicio) * 1000
                print(f" [ÉXITO 4-VÍAS] Protocolo de 4 vías concluido con éxito. Duración total: {rtt:.2f} ms")
            else:
                print(f" [ALERTA] Mensaje inesperado: {msg_resp}")

    except socket.timeout:
        print(f" [ERROR TIMEOUT] Se agotó el tiempo de espera ({timeout}s) sin recibir respuesta.", file=sys.stderr)
    except socket.error as e:
        print(f" [ERROR DE SOCKET] {e}", file=sys.stderr)


def iniciar_proceso_a(ip_local: str, rx_port: int, tx_port: int,
                      dest_ip: str, dest_rx_port: int,
                      protocolo: str, mensaje: str = None, timeout: float = 5.0):
    """
    Inicializa canales desacoplados y administra la ejecución interactiva o en lote de Proceso A.
    """
    canales = CanalesDesacoplados(
        nombre_nodo="PROCESO A",
        bind_ip=ip_local,
        rx_port=rx_port,
        tx_port=tx_port,
        timeout_rx=timeout
    )

    print("=" * 75)
    print(" [PROCESO A - INICIADOR] Activo")
    print(f" [CANAL RX] Escuchando en {ip_local}:{rx_port}")
    print(f" [CANAL TX] Transmitiendo desde {ip_local}:{tx_port}")
    print(f" [DESTINO PROCESO B] {dest_ip}:{dest_rx_port}")
    print(f" [MODO DE PROTOCOLO] {protocolo} Vías")
    print("=" * 75)

    seq_actual = 1

    try:
        if mensaje is not None:
            ejecutar_transaccion(canales, dest_ip, dest_rx_port, mensaje, seq_actual, protocolo, timeout)
        else:
            print(" Modo interactivo iniciado. Escriba la tarea/petición y presione ENTER.")
            print(" Ingrese 'salir' para finalizar.\n")
            while True:
                linea = input(f"\n [Transacción #{seq_actual}] Ingrese mensaje a procesar: ").strip()
                if not linea:
                    continue
                if linea.lower() in ("salir", "exit", "quit"):
                    break
                ejecutar_transaccion(canales, dest_ip, dest_rx_port, linea, seq_actual, protocolo, timeout)
                seq_actual += 1

    except KeyboardInterrupt:
        print("\n [PROCESO A] Cancelado por el usuario (Ctrl+C).")
    finally:
        canales.cerrar()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Proceso A (Iniciador) - Protocolo Petición-Respuesta Multívía (UTN FRCU)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--ip-local", default="0.0.0.0", help="IP local donde enlazar sockets RX y TX")
    parser.add_argument("--rx-port", type=int, default=5001, help="Puerto local exclusivo de recepción (RX)")
    parser.add_argument("--tx-port", type=int, default=5002, help="Puerto local exclusivo de transmisión (TX)")
    parser.add_argument("--dest-ip", default="127.0.0.1", help="IP de Proceso B")
    parser.add_argument("--dest-rx-port", type=int, default=6001, help="Puerto RX de Proceso B")
    parser.add_argument("-p", "--protocolo", choices=["2", "3", "4"], default="2",
                        help="Variante de protocolo (2: Req-Resp, 3: Req-Resp-ACK, 4: Req-ACK-Resp-ACK)")
    parser.add_argument("-m", "--mensaje", type=str, default=None, help="Mensaje inicial a procesar")
    parser.add_argument("-t", "--timeout", type=float, default=5.0, help="Timeout de recepción en segundos")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    iniciar_proceso_a(
        ip_local=args.ip_local,
        rx_port=args.rx_port,
        tx_port=args.tx_port,
        dest_ip=args.dest_ip,
        dest_rx_port=args.dest_rx_port,
        protocolo=args.protocolo,
        mensaje=args.mensaje,
        timeout=args.timeout
    )
