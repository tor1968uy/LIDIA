from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
import time

# --- Configuración ---
KAFKA_TOPIC = "mongo_changes"
KAFKA_SERVER = "localhost:9092"
ES_SERVER = "http://localhost:9201"

# --- Conexiones ---
print("Conectando a Kafka...")
consumidor = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest'
)

print("Conectando a Elasticsearch...")
es = Elasticsearch(ES_SERVER)

while not es.ping():
    print("Esperando a Elasticsearch...")
    time.sleep(1)
print("Conectado a Elasticsearch exitosamente.")

# --- Bucle de Consumo ---
print(f"Escuchando mensajes del topic de Kafka: {KAFKA_TOPIC}...")

try:
    for mensaje in consumidor:
        datos = mensaje.value
        coleccion = datos['coleccion']
        operacion = datos['operacion']
        doc_id = datos['documento_id']
        
        indice_es = coleccion
        
        print(f"\nMensaje recibido: Op={operacion}, Coleccion={coleccion}, ID={doc_id}")

        try:
            if operacion == "insert" or operacion == "update":
                documento = datos['datos_completos']
                if documento:
                    # --- INICIO DE LA SOLUCIÓN ---
                    # El campo '_id' de Mongo no puede ir dentro del 'body' de ES
                    if '_id' in documento:
                        del documento['_id'] # <- LÍNEA AÑADIDA
                    # --- FIN DE LA SOLUCIÓN ---

                    es.index(index=indice_es, id=doc_id, body=documento)
                    print(f"-> Documento ID {doc_id} indexado en ES (índice: {indice_es}).")
                
            elif operacion == "delete":
                if es.exists(index=indice_es, id=doc_id):
                    es.delete(index=indice_es, id=doc_id)
                    print(f"-> Documento ID {doc_id} borrado de ES (índice: {indice_es}).")
                else:
                    print(f"-> Documento ID {doc_id} no encontrado en ES. Se ignora borrado.")

        except Exception as e:
            print(f"!! ERROR al procesar mensaje ID {doc_id}: {e}")
            
except KeyboardInterrupt:
    print("\nDeteniendo el consumidor...")
finally:
    consumidor.close()
    print("Consumidor detenido.")