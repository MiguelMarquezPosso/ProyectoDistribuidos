#broker.py
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

# Definición de tópicos
TOPIC_REGISTRO = "REGISTRO"           # Para registros de taxis
TOPIC_ACTUALIZACION = "ACTUALIZACION" # Para actualizaciones de posición
TOPIC_SERVICIO = "SERVICIO"          # Para asignaciones de servicio
TOPIC_SERVIDOR = "SERVIDOR"           # Para mensajes dirigidos al servidor
TOPIC_TAXI_BASE = "TAXI"             # Base para tópicos individuales de taxis

def get_taxi_topic(taxi_id):
    """Genera el tópico específico para un taxi"""
    return f"{TOPIC_TAXI_BASE}.{taxi_id}"

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
                    try:
                        # Extraer el tópico y el mensaje
                        if len(message) > 1:
                            topic = message[0].decode('utf-8')
                            msg_str = message[-1].decode('utf-8')
                            msg_data = json.loads(msg_str)
                            tipo = msg_data.get('tipo', '')

                            # Procesar según el tipo de mensaje
                            if tipo == 'registro':
                                taxi_id = msg_data['id']
                                # Publicar en el tópico de registro y el tópico específico del taxi
                                new_message = [
                                    f"{TOPIC_REGISTRO}".encode(),
                                    json.dumps(msg_data).encode()
                                ]
                                backend.send_multipart(new_message)
                                print(f"\nBroker: Registro del Taxi {taxi_id} reenviado en tópico {TOPIC_REGISTRO}")

                            elif tipo == 'actualizacion':
                                taxi_id = msg_data['id']
                                # Publicar en tópico de actualizaciones y tópico específico del taxi
                                new_message = [
                                    f"{TOPIC_ACTUALIZACION}".encode(),
                                    json.dumps(msg_data).encode()
                                ]
                                backend.send_multipart(new_message)
                                print(f"Broker: Actualización del Taxi {taxi_id} reenviada en tópico {TOPIC_ACTUALIZACION}")

                            elif tipo == 'servicio_asignado':
                                taxi_id = msg_data['taxi_id']
                                # Publicar en el tópico específico del taxi
                                taxi_topic = get_taxi_topic(taxi_id)
                                new_message = [
                                    f"{taxi_topic}".encode(),
                                    json.dumps(msg_data).encode()
                                ]
                                backend.send_multipart(new_message)
                                print(f"Broker: Asignación de servicio reenviada en tópico {taxi_topic}")

                            else:
                                # Mensajes desconocidos se envían al tópico general del servidor
                                new_message = [
                                    f"{TOPIC_SERVIDOR}".encode(),
                                    json.dumps(msg_data).encode()
                                ]
                                backend.send_multipart(new_message)
                                print(f"Broker: Mensaje desconocido reenviado en tópico {TOPIC_SERVIDOR}")

                    except json.JSONDecodeError as e:
                        print(f"Broker: Error decodificando JSON: {e}")
                        backend.send_multipart(message)
                    except Exception as e:
                        print(f"Broker: Error procesando mensaje: {e}")
                        backend.send_multipart(message)

                if backend in events:
                    message = backend.recv_multipart()
                    topic = message[0].decode('utf-8')
                    print(f"Broker: Nueva suscripción recibida para tópico: {topic}")
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