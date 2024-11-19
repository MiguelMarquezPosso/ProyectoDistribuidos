#base_de_datos
import threading
import time
import json
from pathlib import Path

class BaseDeDatos:
    def __init__(self, archivo="base_de_datos.json"):
        self.archivo = archivo
        self.lock = threading.Lock()
        # Inicializar la estructura de datos base
        self.datos = {
            "taxis": {},
            "historial_posiciones": {},
            "servicios_por_taxi": {},  # Este es el campo que está causando problemas
            "servicios_asignados": [],
            "estadisticas": {
                "servicios_exitosos": 0,
                "servicios_rechazados": 0
            }
        }

        # Asegurarnos de que el archivo existe y tiene la estructura correcta
        if not Path(self.archivo).exists():
            self.guardar_datos()
        else:
            self.cargar_datos()

    def cargar_datos(self):
        try:
            if Path(self.archivo).exists():
                with open(self.archivo, 'r') as f:
                    datos_cargados = json.load(f)
                    # Asegurarse de que todos los campos necesarios existen
                    for campo in ["taxis", "historial_posiciones", "servicios_por_taxi",
                                "servicios_asignados", "estadisticas"]:
                        if campo not in datos_cargados:
                            datos_cargados[campo] = self.datos[campo]
                    self.datos = datos_cargados
        except Exception as e:
            print(f"Error cargando base de datos: {e}")
            # Si hay error, mantener la estructura por defecto
            self.guardar_datos()

    def guardar_datos(self):
        with self.lock:
            try:
                with open(self.archivo, 'w') as f:
                    json.dump(self.datos, f, indent=4)
            except Exception as e:
                print(f"Error guardando base de datos: {e}")

    def registrar_taxi(self, taxi_id, posicion, velocidad):
        with self.lock:
            str_taxi_id = str(taxi_id)
            # Asegurarse de que todas las estructuras necesarias existen
            if "taxis" not in self.datos:
                self.datos["taxis"] = {}
            if "historial_posiciones" not in self.datos:
                self.datos["historial_posiciones"] = {}
            if "servicios_por_taxi" not in self.datos:
                self.datos["servicios_por_taxi"] = {}

            # Registrar el taxi
            self.datos["taxis"][str_taxi_id] = {
                "id": taxi_id,
                "posicion_inicial": posicion,
                "velocidad": velocidad,
                "fecha_registro": time.strftime("%Y-%m-%d %H:%M:%S")
            }

            # Inicializar las estructuras relacionadas
            if str_taxi_id not in self.datos["historial_posiciones"]:
                self.datos["historial_posiciones"][str_taxi_id] = []

            if str_taxi_id not in self.datos["servicios_por_taxi"]:
                self.datos["servicios_por_taxi"][str_taxi_id] = 0

            self.guardar_datos()

    def actualizar_posicion_taxi(self, taxi_id, posicion):
        with self.lock:
            str_taxi_id = str(taxi_id)
            if str_taxi_id not in self.datos["historial_posiciones"]:
                self.datos["historial_posiciones"][str_taxi_id] = []

            self.datos["historial_posiciones"][str_taxi_id].append({
                "posicion": posicion,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            })
            self.guardar_datos()

    def registrar_servicio(self, taxi_id, pos_taxi, pos_usuario):
        with self.lock:
            str_taxi_id = str(taxi_id)
            # Incrementar contador de servicios del taxi
            self.datos["servicios_por_taxi"][str_taxi_id] = \
                self.datos["servicios_por_taxi"].get(str_taxi_id, 0) + 1

            # Registrar detalles del servicio
            self.datos["servicios_asignados"].append({
                "taxi_id": taxi_id,
                "pos_taxi": pos_taxi,
                "pos_usuario": pos_usuario,
                "fecha_hora": time.strftime("%Y-%m-%d %H:%M:%S")
            })

            # Incrementar contador de servicios exitosos
            self.datos["estadisticas"]["servicios_exitosos"] += 1
            self.guardar_datos()

    def registrar_servicio_rechazado(self):
        with self.lock:
            self.datos["estadisticas"]["servicios_rechazados"] += 1
            self.guardar_datos()