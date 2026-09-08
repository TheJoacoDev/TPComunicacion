#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pruebas automatizadas de integración para los Ejercicios 1, 2 y 3.
Verifica que los sockets, los protocolos multívía y la persistencia TCP
funcionen correctamente de extremo a extremo en localhost.
"""

import subprocess
import time
import sys
import os

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

PYTHON_EXEC = sys.executable
ENV_UTF8 = os.environ.copy()
ENV_UTF8["PYTHONIOENCODING"] = "utf-8"

def test_ejercicio1():
    print("\n" + "=" * 60)
    print(">>> Probando Ejercicio 1: Datagramas UDP...")
    print("=" * 60)
    
    # Iniciar receptor UDP en puerto 9100 configurado para 1 mensaje
    proc_rx = subprocess.Popen(
        [PYTHON_EXEC, "-u", "ejercicio1/receptor_udp.py", "-p", "9100", "-n", "1"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        env=ENV_UTF8
    )
    time.sleep(0.6)

    # Iniciar emisor UDP enviando un mensaje puntual
    proc_tx = subprocess.run(
        [PYTHON_EXEC, "-u", "ejercicio1/emisor_udp.py", "-p", "9100", "-m", "Hola UDP Sistemas Distribuidos"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=ENV_UTF8
    )
    print("Salida Emisor UDP:\n", proc_tx.stdout)
    assert proc_tx.returncode == 0, f"Fallo emisor UDP: {proc_tx.stderr}"

    stdout_rx, stderr_rx = proc_rx.communicate(timeout=5)
    print("Salida Receptor UDP:\n", stdout_rx)
    assert "Hola UDP Sistemas Distribuidos" in stdout_rx, f"El mensaje no fue recibido por el receptor UDP. Stderr: {stderr_rx}"
    print("[OK] Ejercicio 1 completado exitosamente.")


def test_ejercicio2_protocolo(protocolo: str):
    print("\n" + "=" * 60)
    print(f">>> Probando Ejercicio 2: Protocolo {protocolo}-Vias...")
    print("=" * 60)

    # Proceso B (Respondedor)
    # RX: 6101, TX: 6102. Destino A: RX 5101
    proc_b = subprocess.Popen(
        [
            PYTHON_EXEC, "-u", "ejercicio2/proceso_b.py",
            "--rx-port", "6101",
            "--tx-port", "6102",
            "--dest-rx-port", "5101",
            "-p", protocolo
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        env=ENV_UTF8
    )
    time.sleep(0.5)

    # Proceso A (Iniciador)
    # RX: 5101, TX: 5102. Destino B: RX 6101
    proc_a = subprocess.run(
        [
            PYTHON_EXEC, "-u", "ejercicio2/proceso_a.py",
            "--rx-port", "5101",
            "--tx-port", "5102",
            "--dest-rx-port", "6101",
            "-p", protocolo,
            "-m", f"Test Carga Util {protocolo} Vias"
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=ENV_UTF8,
        timeout=15
    )

    print(f"Salida Proceso A ({protocolo} vias):\n", proc_a.stdout)
    assert proc_a.returncode == 0, f"Error en Proceso A: {proc_a.stderr}"
    assert f"{protocolo}-V" in proc_a.stdout, f"Protocolo de {protocolo} vias no reporto exito"

    proc_b.terminate()
    stdout_b, _ = proc_b.communicate()
    print(f"Salida Proceso B ({protocolo} vias):\n", stdout_b)
    print(f"[OK] Ejercicio 2 ({protocolo}-Vias) completado exitosamente.")


def test_ejercicio3():
    print("\n" + "=" * 60)
    print(">>> Probando Ejercicio 3: TCP Confiable y Persistente...")
    print("=" * 60)

    # Iniciar servidor TCP en puerto 7100
    proc_servidor = subprocess.Popen(
        [PYTHON_EXEC, "-u", "ejercicio3/servidor_tcp.py", "-p", "7100"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        env=ENV_UTF8
    )
    time.sleep(0.5)

    # Iniciar cliente TCP enviando un mensaje
    proc_cliente = subprocess.run(
        [PYTHON_EXEC, "-u", "ejercicio3/cliente_tcp.py", "-p", "7100", "-m", "Mensaje de prueba TCP Stream"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=ENV_UTF8,
        timeout=10
    )

    print("Salida Cliente TCP:\n", proc_cliente.stdout)
    assert proc_cliente.returncode == 0, f"Error en cliente TCP: {proc_cliente.stderr}"
    assert "ACK_TCP_SERVER: Procesado 'MENSAJE DE PRUEBA TCP STREAM'" in proc_cliente.stdout

    proc_servidor.terminate()
    stdout_serv, _ = proc_servidor.communicate()
    print("Salida Servidor TCP:\n", stdout_serv)
    assert "Mensaje de prueba TCP Stream" in stdout_serv
    print("[OK] Ejercicio 3 completado exitosamente.")


if __name__ == "__main__":
    try:
        test_ejercicio1()
        for proto in ["2", "3", "4"]:
            test_ejercicio2_protocolo(proto)
        test_ejercicio3()
        print("\n" + "=" * 65)
        print(" [EXITO TOTAL] TODAS LAS PRUEBAS DE INTEGRACION FUERON SUPERADAS ")
        print("=" * 65)
    except Exception as e:
        print(f"\n[ERROR] Error durante las pruebas: {e}", file=sys.stderr)
        sys.exit(1)
