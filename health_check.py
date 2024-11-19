import zmq
import time
import threading

# Configuración de puertos para health check
SERVIDOR_CENTRAL_PORT = 5558
SERVIDOR_RESPALDO_PORT = 5557
SERVIDOR_IP = "127.0.0.1"


class HealthChecker:
    def __init__(self):
        self.context = zmq.Context()

        # Socket para el servidor central
        self.socket_central = self.context.socket(zmq.REQ)
        self.socket_central.connect(f"tcp://{SERVIDOR_IP}:{SERVIDOR_CENTRAL_PORT}")

        # Socket para el servidor de respaldo
        self.socket_respaldo = self.context.socket(zmq.REQ)
        self.socket_respaldo.connect(f"tcp://{SERVIDOR_IP}:{SERVIDOR_RESPALDO_PORT}")

        self.servidor_central_activo = True
        self.respaldo_notificado = False

    def verificar_servidor_central(self):
        """Verifica el estado del servidor central"""
        try:
            self.socket_central.send_string("ping", zmq.NOBLOCK)
            poller = zmq.Poller()
            poller.register(self.socket_central, zmq.POLLIN)

            if poller.poll(1000):  # Timeout de 1 segundo
                _ = self.socket_central.recv_string()
                if not self.servidor_central_activo:
                    print("¡Servidor central se ha recuperado!")
                    self.servidor_central_activo = True
                    self.respaldo_notificado = False
                return True
            else:
                raise zmq.ZMQError("Timeout")

        except zmq.ZMQError as e:
            if self.servidor_central_activo:
                print(f"Error en servidor central: {e}")
                self.servidor_central_activo = False

                # Notificar al servidor de respaldo solo una vez
                if not self.respaldo_notificado:
                    self.notificar_respaldo()
                    self.respaldo_notificado = True

            return False

    def notificar_respaldo(self):
        """Notifica al servidor de respaldo que debe activarse"""
        try:
            self.socket_respaldo.send_string("activate")
            _ = self.socket_respaldo.recv_string()
            print("Servidor de respaldo notificado y activado")
        except Exception as e:
            print(f"Error notificando al servidor de respaldo: {e}")

    def monitorear_servidores(self):
        """Monitorea continuamente el estado de los servidores"""
        while True:
            self.verificar_servidor_central()
            time.sleep(1)  # Verificar cada segundo

    def iniciar(self):
        """Inicia el monitoreo de servidores"""
        print("Health Checker iniciado...")
        thread_monitoreo = threading.Thread(target=self.monitorear_servidores)
        thread_monitoreo.daemon = True
        thread_monitoreo.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Cerrando Health Checker...")


def main():
    health_checker = HealthChecker()
    health_checker.iniciar()


if __name__ == "__main__":
    main()