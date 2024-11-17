# taxi.py
import zmq
import time
import sys
import random
import json

# Direcciones y puertos (mantener los existentes)
BROKER_IP = "127.0.0.1"
BROKER_FRONTEND_PORT = 5559
BROKER_BACKEND_PORT = 5560
BROKER_FRONTEND_CONNECT = f"tcp://{BROKER_IP}:{BROKER_FRONTEND_PORT}"
BROKER_BACKEND_CONNECT = f"tcp://{BROKER_IP}:{BROKER_BACKEND_PORT}"

# Definición de tópicos
TOPIC_REGISTRO = "REGISTRO"
TOPIC_ACTUALIZACION = "ACTUALIZACION"
TOPIC_TAXI_BASE = "TAXI"


class Taxi:
    def __init__(self, id_taxi, N, M, pos_inicial, velocidad):
        self.id = id_taxi
        self.N = N
        self.M = M
        self.posicion = pos_inicial
        self.pos_inicial = pos_inicial
        self.velocidad = velocidad
        self.servicios = 0
        self.ocupado = False
        self.ultima_actualizacion = time.time()

        self.context = zmq.Context()

        # Socket para publicar posiciones al broker
        self.socket_pub = self.context.socket(zmq.PUB)
        self.socket_pub.connect(BROKER_FRONTEND_CONNECT)

        time.sleep(1)

        # Socket para recibir asignaciones a través del broker
        self.socket_sub = self.context.socket(zmq.SUB)
        self.socket_sub.connect(BROKER_BACKEND_CONNECT)

        # Suscribirse a tópicos relevantes
        self.taxi_topic = f"{TOPIC_TAXI_BASE}.{self.id}"
        self.socket_sub.setsockopt_string(zmq.SUBSCRIBE, self.taxi_topic)

        time.sleep(1)

        # Enviar mensaje de registro inicial con tópico
        mensaje_registro = {
            'tipo': 'registro',
            'id': self.id,
            'posicion': self.posicion,
            'velocidad': self.velocidad
        }
        self.socket_pub.send_multipart([
            TOPIC_REGISTRO.encode(),
            json.dumps(mensaje_registro).encode()
        ])
        print(f"Taxi {self.id}: Registrado en el sistema en posición {self.posicion}")

    def publicar_posicion(self):
        tiempo_actual = time.time()
        mensaje = {
            'tipo': 'actualizacion',
            'id': self.id,
            'posicion': self.posicion,
            'ocupado': self.ocupado,
            'servicios': self.servicios,
            'timestamp': tiempo_actual
        }
        self.socket_pub.send_multipart([
            TOPIC_ACTUALIZACION.encode(),
            json.dumps(mensaje).encode()
        ])
        print(f"Taxi {self.id}: Nueva posición {self.posicion} | Ocupado: {self.ocupado} | Servicios: {self.servicios}")

    def mover(self):
        if self.ocupado or self.velocidad == 0:
            return

        distancia = (self.velocidad * 0.5)
        celdas = int(distancia)

        if random.choice([True, False]):
            dx = random.choice([-1, 1]) * celdas
            nueva_x = max(0, min(self.N, self.posicion[0] + dx))
            if nueva_x != self.posicion[0]:
                self.posicion = (nueva_x, self.posicion[1])
                return True
        else:
            dy = random.choice([-1, 1]) * celdas
            nueva_y = max(0, min(self.M, self.posicion[1] + dy))
            if nueva_y != self.posicion[1]:
                self.posicion = (self.posicion[0], nueva_y)
                return True

        return False

    def procesar_asignaciones(self):
        try:
            mensaje_raw = self.socket_sub.recv_multipart(flags=zmq.NOBLOCK)
            topic = mensaje_raw[0].decode()
            mensaje = json.loads(mensaje_raw[1].decode())

            if topic == self.taxi_topic and mensaje.get('tipo') == 'servicio_asignado':
                print(f"\nTaxi {self.id}: Recibida asignación de servicio en tópico {topic}")
                print(f"Taxi {self.id}: Usuario {mensaje['id_usuario']} en posición {mensaje['pos_usuario']}")
                print(f"Taxi {self.id}: Mi posición actual {self.posicion}")

                self.ocupado = True
                self.servicios += 1

                self.publicar_posicion()

                print(f"Taxi {self.id}: Iniciando servicio #{self.servicios}")
                time.sleep(30)

                self.posicion = self.pos_inicial
                self.ocupado = False
                print(f"Taxi {self.id}: Servicio completado, volviendo a posición inicial {self.pos_inicial}")
                self.publicar_posicion()

                if self.servicios >= 3:
                    print(f"Taxi {self.id}: Completados todos los servicios del día")
                    return False

        except zmq.Again:
            pass
        except Exception as e:
            print(f"Error procesando asignación en Taxi {self.id}: {e}")

        return True

    def iniciar(self):
        print(f"Taxi {self.id} iniciado en posición {self.posicion}")
        self.publicar_posicion()

        while self.servicios < 3:
            try:
                if not self.procesar_asignaciones():
                    break

                if not self.ocupado and self.velocidad > 0:
                    time.sleep(30)
                    if self.mover():
                        self.publicar_posicion()

            except Exception as e:
                print(f"Error en taxi {self.id}: {e}")

        self.socket_pub.close()
        self.socket_sub.close()
        self.context.term()


def main():
    if len(sys.argv) != 6:
        print("Uso: python taxi.py <id> <N> <M> <x,y> <velocidad>")
        return

    id_taxi = int(sys.argv[1])
    N = int(sys.argv[2])
    M = int(sys.argv[3])
    x, y = map(int, sys.argv[4].split(','))
    velocidad = int(sys.argv[5])

    if not (0 <= x <= N and 0 <= y <= M):
        print("Posición inicial fuera de la cuadrícula")
        return

    if velocidad not in [0, 1, 2, 4]:
        print("Velocidad no válida")
        return

    taxi = Taxi(id_taxi, N, M, (x, y), velocidad)
    taxi.iniciar()


if __name__ == "__main__":
    main()