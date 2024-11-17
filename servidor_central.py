# servidor_central.py
import zmq
import threading
import time

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
class ServidorCentral:
    def __init__(self, N, M):
        self.N = N
        self.M = M
        self.taxis = {}  # {id: {'pos': (x,y), 'ocupado': False, 'servicios': 0}}
        self.context = zmq.Context()
        self.lock = threading.Lock()

        # Socket para recibir actualizaciones de posición de taxis
        self.socket_sub = self.context.socket(zmq.SUB)
        self.socket_sub.connect(BROKER_BACKEND_CONNECT) # 5560
        self.socket_sub.setsockopt_string(zmq.SUBSCRIBE, "")

        # Socket para recibir solicitudes de usuarios
        self.socket_rep = self.context.socket(zmq.REP)
        self.socket_rep.bind(f"tcp://*:{USUARIO_SERVER_PORT}") # 5555

        # Socket para notificar a taxis
        self.socket_pub = self.context.socket(zmq.PUB)
        self.socket_pub.connect(BROKER_FRONTEND_CONNECT) # 5559

    def calcular_distancia(self, pos1, pos2):
        return abs(pos1[0] - pos2[0]) + abs(pos1[1] - pos2[1])

    def encontrar_taxi_cercano(self, pos_usuario):
        taxi_cercano = None
        menor_distancia = float('inf')
        tiempo_actual = time.time()

        with self.lock:
            for id_taxi, info in self.taxis.items():
                # Verificar si el taxi está realmente disponible
                if (not info['ocupado'] and
                        info['servicios'] < 3 and
                        (tiempo_actual - info.get('ultima_asignacion', 0)) > 31):  # 31 segundos para margen

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
            try:
                mensaje = self.socket_rep.recv_json()
                tiempo_inicio = time.time()
                pos_usuario = tuple(mensaje['posicion'])
                id_usuario = mensaje['id_usuario']

                print(f"\nProcesando solicitud del Usuario {id_usuario} en posición {pos_usuario}")

                taxi_id = self.encontrar_taxi_cercano(pos_usuario)
                tiempo_respuesta = time.time() - tiempo_inicio

                if taxi_id is not None:
                    with self.lock:
                        if not self.taxis[taxi_id]['ocupado']:
                            self.taxis[taxi_id]['ocupado'] = True
                            self.taxis[taxi_id]['servicios'] += 1
                            self.taxis[taxi_id]['ultima_asignacion'] = time.time()
                            pos_taxi = self.taxis[taxi_id]['pos']

                            print(f"Servidor: Asignando Taxi {taxi_id} en {pos_taxi} al Usuario {id_usuario}")
                            print(f"Servidor: Taxi {taxi_id} ha realizado {self.taxis[taxi_id]['servicios']} servicios")

                            # Enviar notificación al taxi a través del broker
                            mensaje_asignacion = {
                                'tipo': 'servicio_asignado',
                                'taxi_id': taxi_id,
                                'pos_usuario': pos_usuario,
                                'id_usuario': id_usuario
                            }

                            time.sleep(0.1)

                            self.socket_pub.send_json(mensaje_asignacion)
                            print(f"Servidor: Enviando asignación a través del broker para Taxi {taxi_id}")

                            respuesta = {
                                'exito': True,
                                'taxi_id': taxi_id,
                                'pos_taxi': pos_taxi,
                                'tiempo_respuesta': tiempo_respuesta
                            }
                        else:
                            print(f"Servidor: Taxi {taxi_id} ya fue asignado, buscando otro...")
                            respuesta = {'exito': False, 'tiempo_respuesta': tiempo_respuesta}
                else:
                    print(f"Servidor: No hay taxis disponibles para Usuario {id_usuario}")
                    respuesta = {'exito': False, 'tiempo_respuesta': tiempo_respuesta}

                print(f"Servidor: Enviando asignación para Usuario {id_usuario}")
                self.socket_rep.send_json(respuesta)

            except Exception as e:
                print(f"Error procesando solicitud: {e}")
    def procesar_actualizaciones_taxis(self):
        while True:
            try:
                mensaje = self.socket_sub.recv_json()
                #print(f"Servidor: Mensaje recibido: {mensaje}")  # Debug

                with self.lock:
                    if mensaje.get('tipo') == 'registro':
                        taxi_id = mensaje['id']
                        self.taxis[taxi_id] = {
                            'pos': tuple(mensaje['posicion']),
                            'ocupado': False,
                            'servicios': 0,
                            'velocidad': mensaje.get('velocidad', 0)
                        }
                        print(f"\nServidor: REGISTRADO nuevo Taxi {taxi_id} en posición {mensaje['posicion']}")
                        # Imprimir taxis registrados para debug
                        print(f"Servidor: Taxis registrados actualmente: {list(self.taxis.keys())}")

                    elif mensaje.get('tipo') == 'actualizacion':
                        taxi_id = mensaje['id']
                        if taxi_id in self.taxis:
                            self.taxis[taxi_id].update({
                                'pos': tuple(mensaje['posicion']),
                                'ocupado': mensaje.get('ocupado', False),
                                'servicios': mensaje.get('servicios', 0)
                            })
                            print(f"Servidor: Actualizada posición del Taxi {taxi_id} a {mensaje['posicion']}")
                        else:
                            print(f"Servidor: Recibida actualización de taxi no registrado {taxi_id}")

            except Exception as e:
                print(f"Error procesando mensaje en servidor: {e}")

    def iniciar(self):
        thread_usuarios = threading.Thread(target=self.procesar_solicitudes_usuarios)
        thread_taxis = threading.Thread(target=self.procesar_actualizaciones_taxis)

        thread_usuarios.daemon = True
        thread_taxis.daemon = True

        thread_usuarios.start()
        thread_taxis.start()

        print("Servidor central iniciado...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Cerrando servidor central...")


def main():
    servidor = ServidorCentral(100, 100)  # Ejemplo con ciudad 100x100
    servidor.iniciar()


if __name__ == "__main__":
    main()