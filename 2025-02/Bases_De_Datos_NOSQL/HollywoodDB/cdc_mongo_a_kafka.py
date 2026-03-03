import pymongo
from kafka import KafkaProducer
import json
from bson import json_util
import time

# --- Configuración ---
KAFKA_TOPIC = "mongo_changes"
KAFKA_SERVER = "localhost:9092"
MONGO_URI = "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"

print("=== INICIANDO CDC MONGO → KAFKA ===")
print(f"Conectando a MongoDB: {MONGO_URI}")
print(f"Conectando a Kafka: {KAFKA_SERVER}")

# --- Conexiones ---
try:
    productor = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v, default=json_util.default).encode('utf-8'),
        request_timeout_ms=30000,
        retries=5
    )
    print("Conectado a Kafka exitosamente.")
except Exception as e:
    print(f"********** Error al conectar a Kafka: {e} **********")
    exit()

try:
    client = pymongo.MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000,
        socketTimeoutMS=30000
    )
    db = client["cine_db"]
    print("Conectado a MongoDB exitosamente.")
    
    # Verificar conexión y replica set
    info = client.admin.command('ismaster')
    print(f"Replica Set: {info.get('setName', 'N/A')}")
    print(f"Es primary: {info.get('ismaster', False)}")
    
except Exception as e:
    print(f"********** Error al conectar a MongoDB: {e} **********")
    exit()

def observar_colecciones():
    print("\n Configurando Change Streams...")
    
    colecciones = ['directors', 'peliculas', 'cast']
    streams = []
    
    for coleccion in colecciones:
        try:
            print(f"Iniciando change stream para: {coleccion}")
            pipeline = [{'$match': {'operationType': {'$in': ['insert', 'update', 'delete']}}}]
            ## stream = db[coleccion].watch(pipeline=pipeline) se modifica para 
            stream = db[coleccion].watch(pipeline=pipeline, full_document='updateLookup')
            streams.append((coleccion, stream))
            print(f"Change stream iniciado para: {coleccion}")
        except Exception as e:
            print(f"********** Error iniciando change stream para {coleccion}: {e} **********")
    
    if not streams:
        print("********** No se pudo iniciar ningún change stream **********")
        return
    
    print(f"\n Escuchando cambios en {len(streams)} colecciones...")
    print(" Realiza cambios en MongoDB para ver los eventos")
    
    try:
        while True:
            cambios_detectados = 0
            
            for coleccion, stream in streams:
                try:
                    # Usar try_next() que no bloquea
                    cambio = stream.try_next()
                    
                    if cambio:
                        cambios_detectados += 1
                        print(f"\n CAMBIO DETECTADO en {coleccion.upper()}:")
                        print(f"    Operación: {cambio['operationType']}")
                        print(f"    Documento ID: {cambio['documentKey']['_id']}")
                        
                        # Preparar mensaje para Kafka
                        mensaje = {
                            "coleccion": coleccion,
                            "operacion": cambio['operationType'],
                            "documento_id": str(cambio['documentKey']['_id']),
                            "datos_completos": cambio.get('fullDocument')
                        }
                        
                        print(f"    Enviando a Kafka topic: {KAFKA_TOPIC}")
                        
                        # Enviar a Kafka
                        future = productor.send(KAFKA_TOPIC, mensaje)
                        productor.flush()
                        
                        # Verificar envío
                        try:
                            future.get(timeout=10)
                            print("    Mensaje enviado exitosamente a Kafka")
                        except Exception as e:
                            print(f"   ********** Error enviando a Kafka: {e} **********")
                            
                except Exception as e:
                    print(f"********** Error procesando stream de {coleccion}: {e} **********")
            
            # Solo dormir si no hubo cambios (para no saturar CPU)
            if cambios_detectados == 0:
                time.sleep(1)
                
    except KeyboardInterrupt:
        print("\n Deteniendo CDC...")
    except Exception as e:
        print(f"********** Error general: {e} **********")
    finally:
        print(" Cerrando conexiones...")
        for _, stream in streams:
            stream.close()
        client.close()
        productor.close()
        print(" CDC detenido correctamente")

if __name__ == "__main__":
    observar_colecciones()