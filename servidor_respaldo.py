import zmq
import threading
import time
import json

# Usar las mismas configuraciones que el servidor central
BROKER_IP = "127.0.0.1"
BROKER_FRONTEND_PORT = 5559
BROKER_BACKEND_PORT = 5560
USUARIO_SERVER_PORT = 5556  # Puerto diferente para el servidor de respaldo
BROKER_FRONTEND_CONNECT = f"tcp://{BROKER_IP}:{BROKER_FRONTEND_PORT}"
BROKER_BACKEND_CONNECT = f"tcp://{BROKER_IP}:{BROKER_BACKEND_PORT}"
HEALTH_CHECK_PORT = 5557

# Definición de tópicos
TOPIC_REGISTRO = "REGISTRO"
TOPIC_ACTUALIZACION = "ACTUALIZACION"
TOPIC_SERVICIO = "SERVICIO"
TOPIC_TAXI_BASE = "TAXI"


def get_taxi_topic(taxi_id):
    return f"{TOPIC_TAXI_BASE}.{taxi_id}"


class ServidorRespaldo:
    def __init__(self, N, M):
        self.N = N
        self.M = M
        self.taxis = {}
        self.context = zmq.Context()
        self.lock = threading.Lock()
        self.activo = False  # El servidor de respaldo inicia inactivo

        # Socket para recibir actualizaciones de posición de taxis
        self.socket_sub = self.context.socket(zmq.SUB)
        self.socket_sub.connect(BROKER_BACKEND_CONNECT)

        # Suscribirse a todos los tópicos relevantes
        self.socket_sub.setsockopt_string(zmq.SUBSCRIBE, TOPIC_REGISTRO)
        self.socket_sub.setsockopt_string(zmq.SUBSCRIBE, TOPIC_ACTUALIZACION)

        # Socket para recibir solicitudes de usuarios (inactivo inicialmente)
        self.socket_rep = self.context.socket(zmq.REP)
        self.socket_rep.bind(f"tcp://*:{USUARIO_SERVER_PORT}")

        # Socket para notificar a taxis
        self.socket_pub = self.context.socket(zmq.PUB)
        self.socket_pub.connect(BROKER_FRONTEND_CONNECT)

        # Socket para health check
        self.socket_health = self.context.socket(zmq.REP)
        self.socket_health.bind(f"tcp://*:{HEALTH_CHECK_PORT}")

    def activar_servidor(self):
        """Activa el servidor de respaldo cuando el principal falla"""
        print("\n¡SERVIDOR DE RESPALDO ACTIVADO!")
        with self.lock:
            self.activo = True

    def calcular_distancia(self, pos1, pos2):
        return abs(pos1[0] - pos2[0]) + abs(pos1[1] - pos2[1])

    def encontrar_taxi_cercano(self, pos_usuario):
        taxi_cercano = None
        menor_distancia = float('inf')
        tiempo_actual = time.time()

        with self.lock:
            for id_taxi, info in self.taxis.items():
                if (not info['ocupado'] and
                        info['servicios'] < 3 and
                        (tiempo_actual - info.get('ultima_asignacion', 0)) > 31):

                    distancia = self.calcular_distancia(info['pos'], pos_usuario)
                    if distancia < menor_distancia or (
                            distancia == menor_distancia and
                            (taxi_cercano is None or id_taxi < taxi_cercano)
                    ):
                        menor_distancia = distancia
                        taxi_cercano = id_taxi

        return taxi_cercano

    def procesar_solicitudes_usuarios(self):
        while True:
            if not self.activo:
                time.sleep(1)
                continue

            try:
                mensaje = self.socket_rep.recv_json()
                tiempo_inicio = time.time()
                pos_usuario = tuple(mensaje['posicion'])
                id_usuario = mensaje['id_usuario']

                print(f"\nServidor Respaldo: Procesando solicitud del Usuario {id_usuario} en posición {pos_usuario}")

                taxi_id = self.encontrar_taxi_cercano(pos_usuario)
                tiempo_respuesta = time.time() - tiempo_inicio

                if taxi_id is not None:
                    with self.lock:
                        if not self.taxis[taxi_id]['ocupado']:
                            self.taxis[taxi_id]['ocupado'] = True
                            self.taxis[taxi_id]['servicios'] += 1
                            self.taxis[taxi_id]['ultima_asignacion'] = time.time()
                            pos_taxi = self.taxis[taxi_id]['pos']

                            print(f"Servidor Respaldo: Asignando Taxi {taxi_id} en {pos_taxi} al Usuario {id_usuario}")

                            # Enviar notificación al taxi
                            mensaje_asignacion = {
                                'tipo': 'servicio_asignado',
                                'taxi_id': taxi_id,
                                'pos_usuario': pos_usuario,
                                'id_usuario': id_usuario
                            }

                            taxi_topic = get_taxi_topic(taxi_id)
                            self.socket_pub.send_multipart([
                                taxi_topic.encode(),
                                json.dumps(mensaje_asignacion).encode()
                            ])

                            respuesta = {
                                'exito': True,
                                'taxi_id': taxi_id,
                                'pos_taxi': pos_taxi,
                                'tiempo_respuesta': tiempo_respuesta
                            }
                        else:
                            respuesta = {'exito': False, 'tiempo_respuesta': tiempo_respuesta}
                else:
                    respuesta = {'exito': False, 'tiempo_respuesta': tiempo_respuesta}

                self.socket_rep.send_json(respuesta)

            except Exception as e:
                print(f"Error procesando solicitud en servidor respaldo: {e}")

    def procesar_actualizaciones_taxis(self):
        while True:
            try:
                mensaje_raw = self.socket_sub.recv_multipart()
                topic = mensaje_raw[0].decode()
                mensaje = json.loads(mensaje_raw[1].decode())

                with self.lock:
                    if topic == TOPIC_REGISTRO:
                        taxi_id = mensaje['id']
                        self.taxis[taxi_id] = {
                            'pos': tuple(mensaje['posicion']),
                            'ocupado': False,
                            'servicios': 0,
                            'velocidad': mensaje.get('velocidad', 0)
                        }
                        if self.activo:
                            print(f"\nServidor Respaldo: REGISTRADO Taxi {taxi_id} en posición {mensaje['posicion']}")

                    elif topic == TOPIC_ACTUALIZACION:
                        taxi_id = mensaje['id']
                        if taxi_id in self.taxis:
                            self.taxis[taxi_id].update({
                                'pos': tuple(mensaje['posicion']),
                                'ocupado': mensaje.get('ocupado', False),
                                'servicios': mensaje.get('servicios', 0)
                            })
                            if self.activo:
                                print(
                                    f"Servidor Respaldo: Actualizada posición del Taxi {taxi_id} a {mensaje['posicion']}")

            except Exception as e:
                print(f"Error procesando mensaje en servidor respaldo: {e}")

    def health_check_responder(self):
        """Responde a los mensajes del health checker"""
        while True:
            try:
                _ = self.socket_health.recv_string()
                self.socket_health.send_string("OK")
            except Exception as e:
                print(f"Error en health check del servidor respaldo: {e}")

    def iniciar(self):
        thread_usuarios = threading.Thread(target=self.procesar_solicitudes_usuarios)
        thread_taxis = threading.Thread(target=self.procesar_actualizaciones_taxis)
        thread_health = threading.Thread(target=self.health_check_responder)

        thread_usuarios.daemon = True
        thread_taxis.daemon = True
        thread_health.daemon = True

        thread_usuarios.start()
        thread_taxis.start()
        thread_health.start()

        print("Servidor de respaldo iniciado en modo pasivo...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Cerrando servidor de respaldo...")


def main():
    servidor = ServidorRespaldo(100, 100)
    servidor.iniciar()


if __name__ == "__main__":
    main()