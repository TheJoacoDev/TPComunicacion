#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
Cátedra: Sistemas Distribuidos - UTN FRCU
Ejercicio 2: Protocolo Petición-Respuesta Multívía - Proceso B (Respondedor)
=============================================================================
Descripción pedagógica:
Proceso B actúa como el servidor/receptor de solicitudes. Implementa la regla
estricta de desacople de puertos:
  - Puerto de escucha (RX): 6001 (por defecto)
  - Puerto de transmisión (TX): 6002 (por defecto)

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


def procesar_tarea(payload: str) -> str:
    """Simula el procesamiento de una tarea por parte del servidor."""
    # En este ejemplo pedagógico, invierte la cadena y agrega metadatos del servidor
    return f"PROCESADO_POR_B: '{payload.upper()}' (Longitud: {len(payload)})"


def ejecutar_proceso_b(ip_local: str, rx_port: int, tx_port: int,
                       dest_ip: str, dest_rx_port: int,
                       protocolo: str, timeout_ack: float = 6.0):
    """
    Ejecuta el bucle de servicio de Proceso B según el protocolo seleccionado.
    """
    canales = CanalesDesacoplados(
        nombre_nodo="PROCESO B",
        bind_ip=ip_local,
        rx_port=rx_port,
        tx_port=tx_port,
        timeout_rx=None  # Bucle principal bloqueante a la espera de nuevas solicitudes
    )

    print("=" * 75)
    print(f" [PROCESO B - SERVIDOR] Activo")
    print(f" [CANAL RX] Escuchando solicitudes en {ip_local}:{rx_port}")
    print(f" [CANAL TX] Transmitiendo respuestas desde {ip_local}:{tx_port}")
    print(f" [DESTINO POR DEFECTO] Proceso A en {dest_ip}:{dest_rx_port}")
    print(f" [MODO DE PROTOCOLO] {protocolo} Vías")
    print("=" * 75)

    transaccion_num = 0

    try:
        while True:
            print("\n [ESTADO] Esperando nueva solicitud REQUEST en puerto RX...")
            # Esperar REQUEST en puerto RX
            msg_req, dir_origen = canales.recibir()

            if msg_req.tipo != TipoMensaje.REQUEST:
                print(f" [AVISO] Se descartó mensaje inesperado de tipo {msg_req.tipo}")
                continue

            transaccion_num += 1
            print(f"\n--- [INICIO TRANSACCIÓN #{transaccion_num} | Protocolo: {protocolo} vías] ---")
            print(f" Solicitud recibida: {msg_req.payload}")
            print(f" Origen del paquete: {dir_origen[0]}:{dir_origen[1]}")

            # =================================================================
            # PROTOCOLO DE 2 VÍAS (Request -> Response)
            # =================================================================
            if protocolo == "2":
                # 1. B procesa la solicitud
                resultado = procesar_tarea(str(msg_req.payload))
                time.sleep(0.3)  # Simula cómputo breve

                # 2. B envía RESPONSE desde TX hacia A (dest_rx_port)
                msg_resp = MensajeProtocolo(
                    tipo=TipoMensaje.RESPONSE,
                    seq_id=msg_req.seq_id,
                    emisor="Proceso_B",
                    payload=resultado
                )
                canales.enviar(msg_resp, dest_ip, dest_rx_port)
                print(f" [FIN TRANSACCIÓN #{transaccion_num}] Respuesta enviada. Flujo de 2 vías completado.")

            # =================================================================
            # PROTOCOLO DE 3 VÍAS (Request -> Response -> ACK)
            # =================================================================
            elif protocolo == "3":
                # 1. B procesa la solicitud
                resultado = procesar_tarea(str(msg_req.payload))
                time.sleep(0.3)

                # 2. B envía RESPONSE desde TX hacia A
                msg_resp = MensajeProtocolo(
                    tipo=TipoMensaje.RESPONSE,
                    seq_id=msg_req.seq_id,
                    emisor="Proceso_B",
                    payload=resultado
                )
                canales.enviar(msg_resp, dest_ip, dest_rx_port)

                # 3. B espera confirmación ACK en RX
                print(f" [ESPERA ACK] Aguardando confirmación final (ACK_RESPONSE) de A...")
                canales.sock_rx.settimeout(timeout_ack)
                try:
                    msg_ack, _ = canales.recibir()
                    if msg_ack.tipo == TipoMensaje.ACK_RES and msg_ack.seq_id == msg_req.seq_id:
                        print(f" [ACK RECIBIDO] Proceso A confirmó recepción de la respuesta. Transacción consolidada.")
                    else:
                        print(f" [ALERTA] Mensaje recibido no coincide con el ACK esperado: {msg_ack}")
                except socket.timeout:
                    print(f" [TIMEOUT] No se recibió ACK de A en {timeout_ack}s. Transacción no confirmada.")
                finally:
                    canales.sock_rx.settimeout(None)  # Restaurar a bloqueante

                print(f" [FIN TRANSACCIÓN #{transaccion_num}] Flujo de 3 vías completado.")

            # =================================================================
            # PROTOCOLO DE 4 VÍAS (Request -> ACK -> Response -> ACK)
            # =================================================================
            elif protocolo == "4":
                # 1. B envía inmediatamente acuse de recibo de la solicitud (ACK_REQUEST)
                msg_ack_req = MensajeProtocolo(
                    tipo=TipoMensaje.ACK_REQ,
                    seq_id=msg_req.seq_id,
                    emisor="Proceso_B",
                    payload={"status": "RECEIVED_PROCESSING", "tiempo_estimado_ms": 1000}
                )
                canales.enviar(msg_ack_req, dest_ip, dest_rx_port)
                print(f" [4-VÍAS Vía 2/4] Acuse de recibo de solicitud (ACK_REQUEST) despachado.")

                # 2. B realiza cómputo intensivo/asíncrono
                print(" [4-VÍAS] Simulando procesamiento intensivo de la tarea (1 segundo)...")
                time.sleep(1.0)
                resultado = procesar_tarea(str(msg_req.payload))

                # 3. B envía el resultado definitivo (RESPONSE)
                msg_resp = MensajeProtocolo(
                    tipo=TipoMensaje.RESPONSE,
                    seq_id=msg_req.seq_id,
                    emisor="Proceso_B",
                    payload=resultado
                )
                canales.enviar(msg_resp, dest_ip, dest_rx_port)
                print(f" [4-VÍAS Vía 3/4] Resultado definitivo (RESPONSE) enviado.")

                # 4. B espera acuse de recibo final de la respuesta (ACK_RESPONSE)
                print(f" [4-VÍAS Vía 4/4] Esperando confirmación final (ACK_RESPONSE) de A...")
                canales.sock_rx.settimeout(timeout_ack)
                try:
                    msg_ack_res, _ = canales.recibir()
                    if msg_ack_res.tipo == TipoMensaje.ACK_RES and msg_ack_res.seq_id == msg_req.seq_id:
                        print(f" [TRANSACCIÓN COMPLETADA] Proceso A confirmó la recepción final exitosamente.")
                    else:
                        print(f" [ALERTA] Mensaje inesperado: {msg_ack_res}")
                except socket.timeout:
                    print(f" [TIMEOUT] No se recibió ACK_RESPONSE en {timeout_ack}s.")
                finally:
                    canales.sock_rx.settimeout(None)

                print(f" [FIN TRANSACCIÓN #{transaccion_num}] Flujo de 4 vías completado.")

    except KeyboardInterrupt:
        print("\n [PROCESO B] Interrupción por teclado (Ctrl+C). Finalizando...")
    finally:
        canales.cerrar()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Proceso B (Respondedor) - Protocolo Petición-Respuesta Multívía (UTN FRCU)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--ip-local", default="0.0.0.0", help="IP local donde enlazar sockets RX y TX")
    parser.add_argument("--rx-port", type=int, default=6001, help="Puerto local exclusivo de recepción (RX)")
    parser.add_argument("--tx-port", type=int, default=6002, help="Puerto local exclusivo de transmisión (TX)")
    parser.add_argument("--dest-ip", default="127.0.0.1", help="IP de Proceso A")
    parser.add_argument("--dest-rx-port", type=int, default=5001, help="Puerto RX de Proceso A")
    parser.add_argument("-p", "--protocolo", choices=["2", "3", "4"], default="2",
                        help="Variante de protocolo a utilizar (2: Req-Resp, 3: Req-Resp-ACK, 4: Req-ACK-Resp-ACK)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    ejecutar_proceso_b(
        ip_local=args.ip_local,
        rx_port=args.rx_port,
        tx_port=args.tx_port,
        dest_ip=args.dest_ip,
        dest_rx_port=args.dest_rx_port,
        protocolo=args.protocolo
    )
