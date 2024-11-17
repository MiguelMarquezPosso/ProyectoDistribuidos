#taxi.py
import zmq
import time
import sys
import random
import threading
import json

# Direcciones IP de los componentes
BROKER_IP = "127.0.0.1"  # IP del broker
SERVIDOR_IP = "127.0.0.1"  # IP del servidor central
TAXI_IP = "127.0.0.1"  # IP base para taxis
USUARIO_IP = "127.0.0.1"  # IP base para usuarios

# Puertos del sistema
BROKER_FRONTEND_PORT = 5559  # Para publicadores (taxis y servidor)
BROKER_BACKEND_PORT = 5560  # Para suscriptores (taxis y servidor)
USUARIO_SERVER_PORT = 5555  # Para comunicación usuario-servidor

# Configuraciones completas
BROKER_FRONTEND_URL = f"tcp://*:{BROKER_FRONTEND_PORT}"
BROKER_BACKEND_URL = f"tcp://*:{BROKER_BACKEND_PORT}"
BROKER_FRONTEND_CONNECT = f"tcp://{BROKER_IP}:{BROKER_FRONTEND_PORT}"
BROKER_BACKEND_CONNECT = f"tcp://{BROKER_IP}:{BROKER_BACKEND_PORT}"
USUARIO_SERVER_URL = f"tcp://{SERVIDOR_IP}:{USUARIO_SERVER_PORT}"

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
        self.running = True

        self.context = zmq.Context()

        # Socket para publicar posiciones
        self.socket_pub = self.context.socket(zmq.PUB)
        self.socket_pub.connect(BROKER_FRONTEND_CONNECT)

        # Socket para recibir asignaciones
        self.socket_sub = self.context.socket(zmq.SUB)
        self.socket_sub.connect(BROKER_BACKEND_CONNECT)

        # Suscribirse al tópico específico del taxi
        self.taxi_topic = f"{TOPIC_TAXI_BASE}.{self.id}"
        self.socket_sub.setsockopt_string(zmq.SUBSCRIBE, self.taxi_topic)

        # Asegurar que las conexiones estén establecidas
        time.sleep(0.5)

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
        mensaje = {
            'tipo': 'actualizacion',
            'id': self.id,
            'posicion': self.posicion,
            'ocupado': self.ocupado,
            'servicios': self.servicios,
            'timestamp': time.time()
        }
        self.socket_pub.send_multipart([
            TOPIC_ACTUALIZACION.encode(),
            json.dumps(mensaje).encode()
        ])
        print(f"Taxi {self.id}: Nueva posición {self.posicion} | Ocupado: {self.ocupado} | Servicios: {self.servicios}")

    def mover(self):
        if self.ocupado or self.velocidad == 0:
            return False

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

    def escuchar_asignaciones(self):
        while self.running:
            try:
                mensaje_raw = self.socket_sub.recv_multipart()
                topic = mensaje_raw[0].decode()
                mensaje = json.loads(mensaje_raw[1].decode())

                if (topic == self.taxi_topic and
                        mensaje.get('tipo') == 'servicio_asignado' and
                        not self.ocupado):
                    print(f"\nTaxi {self.id}: Recibida asignación de servicio en tópico {topic}")
                    print(f"Taxi {self.id}: Usuario {mensaje['id_usuario']} en posición {mensaje['pos_usuario']}")

                    self.ocupado = True
                    self.servicios += 1
                    self.publicar_posicion()

                    # Iniciar el servicio en un nuevo hilo
                    threading.Thread(target=self.realizar_servicio).start()

            except Exception as e:
                if self.running:
                    print(f"Error escuchando asignaciones en Taxi {self.id}: {e}")

    def realizar_servicio(self):
        print(f"Taxi {self.id}: Iniciando servicio #{self.servicios}")
        time.sleep(30)  # Duración del servicio

        # Volver a posición inicial
        self.posicion = self.pos_inicial
        self.ocupado = False
        print(f"Taxi {self.id}: Servicio completado, volviendo a posición inicial {self.pos_inicial}")
        self.publicar_posicion()

        if self.servicios >= 3:
            print(f"Taxi {self.id}: Completados todos los servicios del día")
            self.running = False

    def iniciar(self):
        print(f"Taxi {self.id} iniciado en posición {self.posicion}")
        self.publicar_posicion()

        # Iniciar hilo para escuchar asignaciones
        thread_asignaciones = threading.Thread(target=self.escuchar_asignaciones)
        thread_asignaciones.daemon = True
        thread_asignaciones.start()

        # Bucle principal para movimiento
        while self.running and self.servicios < 3:
            try:
                if not self.ocupado and self.velocidad > 0:
                    time.sleep(30)  # Esperar 30 segundos
                    if self.mover():
                        self.publicar_posicion()
            except Exception as e:
                print(f"Error en bucle principal del taxi {self.id}: {e}")

        self.running = False
        thread_asignaciones.join(timeout=1)

        # Cerrar conexiones
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