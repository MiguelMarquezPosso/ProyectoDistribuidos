# usuario.py
import zmq
import threading
import time
import sys

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

class Usuario(threading.Thread):
    def __init__(self, id_usuario, pos_inicial, tiempo_espera, N, M):
        super().__init__()
        self.id = id_usuario
        self.posicion = pos_inicial
        self.tiempo_espera = tiempo_espera
        self.N = N
        self.M = M

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(USUARIO_SERVER_URL) # 5555

    def solicitar_taxi(self):
        try:
            print(f"\nUsuario {self.id}: Iniciando solicitud de taxi desde posición {self.posicion}")
            tiempo_inicio = time.time()

            self.socket.send_json({
                'tipo': 'solicitud',
                'id_usuario': self.id,
                'posicion': self.posicion,
                'tiempo_solicitud': tiempo_inicio
            })

            poller = zmq.Poller()
            poller.register(self.socket, zmq.POLLIN)

            if poller.poll(5000):  # Timeout de 5 segundos
                respuesta = self.socket.recv_json()
                tiempo_respuesta = respuesta.get('tiempo_respuesta', time.time() - tiempo_inicio)

                if respuesta['exito']:
                    print(f"Usuario {self.id}: Taxi {respuesta['taxi_id']} asignado desde "
                          f"posición {respuesta['pos_taxi']}")
                    print(f"Usuario {self.id}: Tiempo de respuesta del servidor: {tiempo_respuesta:.3f} segundos")
                    return True
                else:
                    print(f"Usuario {self.id}: No hay taxis disponibles")
                    print(f"Usuario {self.id}: Tiempo de respuesta del servidor: {tiempo_respuesta:.3f} segundos")
            else:
                print(f"Usuario {self.id}: Timeout en la solicitud después de 5 segundos")

            return False

        except Exception as e:
            print(f"Error en solicitud de usuario {self.id}: {e}")
            return False

    def run(self):
        print(f"Usuario {self.id}: Iniciado en posición {self.posicion}")
        print(f"Usuario {self.id}: Esperando {self.tiempo_espera} segundos antes de solicitar taxi")

        time.sleep(self.tiempo_espera)

        if self.solicitar_taxi():
            print(f"Usuario {self.id}: Iniciando servicio de 30 segundos")
            time.sleep(30)  # Simular duración del servicio
            print(f"Usuario {self.id}: Servicio completado")
        else:
            print(f"Usuario {self.id}: No se pudo obtener servicio, buscando otra alternativa")

        self.socket.close()
        print(f"Usuario {self.id}: Sesión terminada")


def crear_usuarios(num_usuarios, N, M, archivo_posiciones):
    usuarios = []
    with open(archivo_posiciones, 'r') as f:
        for i, linea in enumerate(f):
            if i >= num_usuarios:
                break
            x, y = map(int, linea.strip().split())
            tiempo_espera = (i + 1) * 5  # Tiempo diferente para cada usuario
            usuario = Usuario(i, (x, y), tiempo_espera, N, M)
            usuarios.append(usuario)

    return usuarios


def main():
    if len(sys.argv) != 5:
        print("Uso: python usuario.py <num_usuarios> <N> <M> <archivo_posiciones>")
        return

    num_usuarios = int(sys.argv[1])
    N = int(sys.argv[2])
    M = int(sys.argv[3])
    archivo_posiciones = sys.argv[4]

    usuarios = crear_usuarios(num_usuarios, N, M, archivo_posiciones)

    for usuario in usuarios:
        usuario.start()

    for usuario in usuarios:
        usuario.join()


if __name__ == "__main__":
    main()

