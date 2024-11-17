# broker.py
import json
import zmq

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


def main():
    try:
        context = zmq.Context()

        # Socket frontend para recibir mensajes de los publicadores
        frontend = context.socket(zmq.XSUB)
        frontend.bind(BROKER_FRONTEND_URL)

        # Socket backend para enviar mensajes a los suscriptores
        backend = context.socket(zmq.XPUB)
        backend.bind(BROKER_BACKEND_URL)

        print("Broker iniciado. Esperando mensajes...")

        poller = zmq.Poller()
        poller.register(frontend, zmq.POLLIN)
        poller.register(backend, zmq.POLLIN)

        while True:
            try:
                events = dict(poller.poll())

                if frontend in events:
                    message = frontend.recv_multipart()
                    # Imprimir mensaje para depuración
                    #print(f"Broker: Mensaje recibido: {message}")

                    try:
                        # El mensaje real está en la última parte del mensaje multipart
                        msg_str = message[-1].decode('utf-8')
                        msg_data = json.loads(msg_str)

                        tipo = msg_data.get('tipo', '')
                        #print(f"Broker: Tipo de mensaje recibido: {tipo}")

                        if tipo == 'registro':
                            print(
                                f"\nBroker: Recibido registro del Taxi {msg_data['id']} en posición {msg_data['posicion']}")
                            # Asegurar que el mensaje se reenvía
                            backend.send_multipart(message)
                            print(f"Broker: Mensaje de registro reenviado")

                        elif tipo == 'actualizacion':
                            print(
                                f"Broker: Recibida actualización del Taxi {msg_data['id']} en posición {msg_data['posicion']}")
                            # Asegurar que el mensaje se reenvía
                            backend.send_multipart(message)
                            print(f"Broker: Mensaje de actualización reenviado")

                        elif tipo == 'servicio_asignado':
                            print(
                                f"Broker: Recibida asignación de servicio al Taxi {msg_data['taxi_id']} para Usuario {msg_data['id_usuario']}")
                            # Asegurar que el mensaje se reenvía
                            backend.send_multipart(message)
                            print(f"Broker: Mensaje de actualización reenviado")

                        else:
                            print(f"Broker: Recibido mensaje desconocido")
                            # Asegurar que el mensaje se reenvía
                            backend.send_multipart(message)
                            print(f"Broker: Mensaje desconocido reenviado")

                    except json.JSONDecodeError as e:
                        print(f"Broker: Error decodificando JSON: {e}")
                        # Asegurar que el mensaje se reenvía
                        backend.send_multipart(message)
                    except Exception as e:
                        print(f"Broker: Error procesando mensaje: {e}")
                        # Asegurar que el mensaje se reenvía
                        backend.send_multipart(message)

                if backend in events:
                    message = backend.recv_multipart()
                    print(f"Broker: Nueva suscripción recibida: {message}")
                    frontend.send_multipart(message)

            except Exception as e:
                print(f"Error en ciclo principal del broker: {e}")

    except Exception as e:
        print(f"Error crítico en el broker: {e}")
    finally:
        frontend.close()
        backend.close()
        context.term()


if __name__ == "__main__":
    main()