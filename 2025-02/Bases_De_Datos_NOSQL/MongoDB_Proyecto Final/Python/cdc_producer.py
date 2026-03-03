import pymongo
from kafka import KafkaProducer
import json
import time

# --- Configuración ---
MONGO_URI = "mongodb://admin:admin123@localhost:27018/UniversoDB?tls=true&tlsCAFile=../certs/ca.crt&tlsAllowInvalidHostnames=true&authSource=admin"
#MONGO_URI = "mongodb://admin:admin123@localhost:27018/"
DB_NAME = "UniversoDB"
KAFKA_BROKER = "kafka:9092"
TOPIC_PREFIX = "mongo_cdc_"
EXCLUDED_COLLECTIONS = {"ImagenesGridFS.files", "ImagenesGridFS.chunks"}

# --- Inicialización ---
client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
    acks='all',
    retries=5,
)

def start_cdc_producer():
    """Monitorea los cambios en todas las colecciones excepto las excluidas."""
    print(f"=== Iniciando CDC para la base '{DB_NAME}' ===")

    try:
        # Escucha toda la base de datos (Change Stream global)
        with db.watch(full_document='updateLookup') as stream:
            print("Esperando eventos de cambio...\n")
            for change in stream:
                ns = change.get("ns", {})
                collection_name = ns.get("coll", "")
                if not collection_name:
                    continue

                # Saltar colecciones excluidas
                if collection_name in EXCLUDED_COLLECTIONS:
                    continue

                topic_name = TOPIC_PREFIX + collection_name.lower()

                message = {
                    "operationType": change.get("operationType"),
                    "collection": collection_name,
                    "documentKey": change.get("documentKey"),
                    "fullDocument": change.get("fullDocument"),
                    "updateDescription": change.get("updateDescription"),
                    "clusterTime": str(change.get("clusterTime"))
                }

                # Enviar evento a Kafka
                producer.send(topic_name, value=message)
                producer.flush()

                print(f"[{time.strftime('%H:%M:%S')}] Evento {message['operationType']} → {topic_name}")

    except pymongo.errors.PyMongoError as e:
        print(f"Error MongoDB: {e}")
        print("⚠️ Asegúrate de que MongoDB sea un replica set o sharded cluster.")
    except Exception as e:
        print(f"Error inesperado: {e}")
    finally:
        producer.close()
        client.close()
        print("CDC detenido correctamente.")

if __name__ == "__main__":
    start_cdc_producer()
