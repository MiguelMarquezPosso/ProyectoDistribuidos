# taxi.py
import zmq
import time
import sys
import random

# Direcciones IP de los componentes
BROKER_IP = "127.0.0.1"       # IP del broker
SERVIDOR_IP = "127.0.0.1"    # IP del servidor central
TAXI_IP = "127.0.0.1"      # IP base para taxis
USUARIO_IP = "127.0.0.1"     # IP base para usuarios

# Puertos del sistema
BROKER_FRONTEND_PORT = 5559       # Para publicadores (taxis y servidor)
BROKER_BACKEND_PORT = 5560        # Para suscriptores (taxis y servidor)
USUARIO_SERVER_PORT = 5555        # Para comunicación usuario-servidor

# Configuraciones completas
BROKER_FRONTEND_URL = f"tcp://*:{BROKER_FRONTEND_PORT}"
BROKER_BACKEND_URL = f"tcp://*:{BROKER_BACKEND_PORT}"
BROKER_FRONTEND_CONNECT = f"tcp://{BROKER_IP}:{BROKER_FRONTEND_PORT}"
BROKER_BACKEND_CONNECT = f"tcp://{BROKER_IP}:{BROKER_BACKEND_PORT}"
USUARIO_SERVER_URL = f"tcp://{SERVIDOR_IP}:{USUARIO_SERVER_PORT}"

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
        self.socket_pub.connect(BROKER_FRONTEND_CONNECT) # 5559

        time.sleep(1)

        # Socket para recibir asignaciones a través del broker
        self.socket_sub = self.context.socket(zmq.SUB)
        self.socket_sub.connect(BROKER_BACKEND_CONNECT) # 5560
        self.socket_sub.setsockopt_string(zmq.SUBSCRIBE, "")

        time.sleep(1)

        # Enviar mensaje de registro inicial
        mensaje_registro = {
            'tipo': 'registro',
            'id': self.id,
            'posicion': self.posicion,
            'velocidad': self.velocidad
        }
        self.socket_pub.send_json(mensaje_registro)
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
        self.socket_pub.send_json(mensaje)
        print(f"Taxi {self.id}: Nueva posición {self.posicion} | Ocupado: {self.ocupado} | Servicios: {self.servicios}")

    def mover(self):
        if self.ocupado or self.velocidad == 0:
            return

        # Calcular distancia a mover basada en la velocidad
        # velocidad es km/h, y queremos mover cada 30 minutos
        distancia = (self.velocidad * 0.5)  # distancia en km por 30 minutos
        celdas = int(distancia)  # cada celda es 1km

        # Decidir dirección aleatoria (vertical u horizontal)
        if random.choice([True, False]):  # Movimiento horizontal
            dx = random.choice([-1, 1]) * celdas
            nueva_x = max(0, min(self.N, self.posicion[0] + dx))
            if nueva_x != self.posicion[0]:  # Solo actualizar si realmente se movió
                self.posicion = (nueva_x, self.posicion[1])
                return True
        else:  # Movimiento vertical
            dy = random.choice([-1, 1]) * celdas
            nueva_y = max(0, min(self.M, self.posicion[1] + dy))
            if nueva_y != self.posicion[1]:  # Solo actualizar si realmente se movió
                self.posicion = (self.posicion[0], nueva_y)
                return True

        return False

    def procesar_asignaciones(self):
        try:
            mensaje = self.socket_sub.recv_json(flags=zmq.NOBLOCK)
            if (mensaje.get('tipo') == 'servicio_asignado' and
                    mensaje.get('taxi_id') == self.id):

                print(f"\nTaxi {self.id}: Recibida asignación de servicio")
                print(f"Taxi {self.id}: Usuario {mensaje['id_usuario']} en posición {mensaje['pos_usuario']}")
                print(f"Taxi {self.id}: Mi posición actual {self.posicion}")

                self.ocupado = True
                self.servicios += 1

                # Notificar que estamos ocupados
                self.publicar_posicion()

                print(f"Taxi {self.id}: Iniciando servicio #{self.servicios}")
                time.sleep(30)  # Duración del servicio

                # Volver a posición inicial
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
                    time.sleep(30)  # Esperar 30 segundos (30 minutos simulados)
                    self.mover()
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