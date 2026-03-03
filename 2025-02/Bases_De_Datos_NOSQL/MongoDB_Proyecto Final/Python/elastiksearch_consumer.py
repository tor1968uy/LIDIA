from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
import time
from collections import deque

# --- Configuración ---
KAFKA_BROKER = "kafka:9092"
TOPIC_PATTERN = "^mongo_cdc_.*$"
ES_HOST = "http://localhost:9200"
GROUP_ID = "es-indexer"

EXCLUDED_TOPICS = {
    "mongo_cdc_imagenesgridfs.chunks",
    "mongo_cdc_imagenesgridfs.files"
}

# --- DEPENDENCIAS SEGÚN TU ESQUEMA ---
# Formato: "colección_hija": ("colección_padre", "campo_referencia")
DEPENDENCIES = {
    # Objetos Naturales
    "estrellas": ("galaxias", "galaxia_id"),
    "planetas": ("estrellas", "estrella_id"),
    "satelitesnaturales": ("planetas", "planeta_id"),
    "cometas": ("astronomos", "descubridor_id"),

    # Objetos Artificiales
    "astronomos": ("observatorios", "observatorio_id"),
    "misionesespaciales": ("agenciasespaciales", "agencia_id"),
    "satelitesartificiales": ("misionesespaciales", "mision_id"),
    "eventosastronomicos": ("observatorios", "observatorio_registrador_id"),
}

# Cola de reintentos
pending_queue = deque()
MAX_RETRIES = 15
RETRY_DELAY = 2  # segundos entre reintentos
BATCH_PROCESS_INTERVAL = 5  # procesar cola cada 5 segundos

# Estadísticas
stats = {
    "procesados": 0,
    "en_cola": 0,
    "fallidos": 0,
    "por_coleccion": {}
}

# --- Conexión a Elasticsearch ---
es = Elasticsearch([ES_HOST])
print("⏳ Esperando conexión a Elasticsearch...")
while not es.ping():
    time.sleep(2)
print("✅ Conectado a Elasticsearch.\n")

# --- Configurar consumidor ---
print(f"⏳ Configurando consumer de Kafka...")
consumer = KafkaConsumer(
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id=GROUP_ID,
    enable_auto_commit=True,
    auto_commit_interval_ms=1000,
    # Configuración de baja latencia
    fetch_min_bytes=1,
    fetch_max_wait_ms=100,
    max_poll_records=100,
    session_timeout_ms=10000,
    heartbeat_interval_ms=3000,
)

consumer.subscribe(pattern=TOPIC_PATTERN)
print(f"✅ Suscrito a topics: {TOPIC_PATTERN}")


def get_parent_reference(collection, doc):
    """Extrae el ID del padre desde el documento"""
    if collection not in DEPENDENCIES:
        return None, None

    parent_collection, parent_field = DEPENDENCIES[collection]

    if parent_field not in doc:
        return parent_collection, None

    parent_value = doc[parent_field]

    # Manejar ObjectId de MongoDB
    if isinstance(parent_value, dict) and "$oid" in parent_value:
        return parent_collection, parent_value["$oid"]

    return parent_collection, str(parent_value)


def check_parent_exists(collection, doc):
    """Verifica si el documento padre existe en Elasticsearch"""
    parent_collection, parent_id = get_parent_reference(collection, doc)

    # Sin dependencias
    if parent_collection is None:
        return True

    # Referencia inválida
    if parent_id is None:
        return True

    # Verificar existencia en Elasticsearch
    try:
        exists = es.exists(index=parent_collection, id=parent_id)
        if not exists:
            print(f"   ⏳ Esperando {parent_collection}/{parent_id}")
        return exists
    except Exception as e:
        print(f"   ❌ Error verificando padre: {e}")
        return False


def process_message(msg, retry_count=0):
    """Procesa un mensaje de Kafka con manejo de dependencias"""
    topic = msg.topic.lower()
    if topic in EXCLUDED_TOPICS:
        return True

    data = msg.value
    op = data.get("operationType")
    coll = data.get("collection")
    doc = data.get("fullDocument")
    doc_key = data.get("documentKey", {}).get("_id")

    if not coll or not doc_key:
        return True

    index_name = coll.lower()
    doc_id = str(doc_key["$oid"]) if isinstance(doc_key, dict) and "$oid" in doc_key else str(doc_key)

    # Actualizar estadísticas
    if index_name not in stats["por_coleccion"]:
        stats["por_coleccion"][index_name] = {"procesados": 0, "pendientes": 0}

    try:
        if op in ("insert", "update", "replace"):
            # Verificar dependencias
            if not check_parent_exists(coll, doc):
                if retry_count < MAX_RETRIES:
                    if retry_count == 0:  # Solo en el primer intento
                        stats["por_coleccion"][index_name]["pendientes"] += 1
                    return False  # No procesado, reintentar
                else:
                    print(f"   ❌ MAX_RETRIES ({MAX_RETRIES}) alcanzado para {coll}/{doc_id}")
                    stats["fallidos"] += 1
                    return True  # Descartar

            # Remover _id de MongoDB
            if doc and "_id" in doc:
                del doc["_id"]

            # Indexar en Elasticsearch
            es.index(index=index_name, id=doc_id, document=doc)

            # Logs y estadísticas
            retry_msg = f" (después de {retry_count} reintentos)" if retry_count > 0 else ""
            timestamp = time.strftime('%H:%M:%S')
            print(f"✅ [{timestamp}] {coll}/{doc_id}{retry_msg}")

            stats["procesados"] += 1
            stats["por_coleccion"][index_name]["procesados"] += 1
            if retry_count > 0:
                stats["por_coleccion"][index_name]["pendientes"] -= 1

            return True

        elif op == "delete":
            if es.exists(index=index_name, id=doc_id):
                es.delete(index=index_name, id=doc_id)
                timestamp = time.strftime('%H:%M:%S')
                print(f"🗑️  [{timestamp}] {coll}/{doc_id}")
                stats["procesados"] += 1
            return True

    except Exception as e:
        print(f"   ❌ Error procesando {coll}/{doc_id}: {e}")
        return retry_count >= MAX_RETRIES


def print_stats():
    """Imprime estadísticas del procesamiento"""
    print("\n" + "=" * 70)
    print("📊 ESTADÍSTICAS DE PROCESAMIENTO")
    print("=" * 70)
    print(f"Total procesados:     {stats['procesados']}")
    print(f"En cola de reintentos: {len(pending_queue)}")
    print(f"Fallidos (descartados): {stats['fallidos']}")
    print("\nPor colección:")
    for coll, data in sorted(stats["por_coleccion"].items()):
        pendientes = data.get("pendientes", 0)
        if data["procesados"] > 0 or pendientes > 0:
            print(f"  • {coll:25s} → Procesados: {data['procesados']:6d} | Pendientes: {pendientes:4d}")
    print("=" * 70 + "\n")


# --- Bucle principal ---
print("=" * 70)
print("🚀 INICIANDO PROCESAMIENTO DE MENSAJES")
print("=" * 70 + "\n")

last_retry_time = time.time()
last_stats_time = time.time()
message_count = 0

try:
    for msg in consumer:
        message_count += 1

        # Procesar mensaje nuevo
        if not process_message(msg):
            pending_queue.append({
                "msg": msg,
                "retry_count": 0,
                "added_at": time.time()
            })
            stats["en_cola"] = len(pending_queue)

        # Procesar cola de reintentos
        current_time = time.time()
        if pending_queue and (current_time - last_retry_time) >= RETRY_DELAY:
            retry_batch_size = len(pending_queue)

            if retry_batch_size > 0:
                print(f"\n🔄 Procesando {retry_batch_size} mensajes en cola de reintentos...")

            for _ in range(retry_batch_size):
                if not pending_queue:
                    break

                pending_item = pending_queue.popleft()
                pending_msg = pending_item["msg"]
                retry_count = pending_item["retry_count"] + 1

                if not process_message(pending_msg, retry_count):
                    # Volver a encolar
                    pending_queue.append({
                        "msg": pending_msg,
                        "retry_count": retry_count,
                        "added_at": pending_item["added_at"]
                    })

            stats["en_cola"] = len(pending_queue)
            last_retry_time = current_time

            if pending_queue:
                print(f"   ⏳ Quedan {len(pending_queue)} mensajes en cola\n")

        # Mostrar estadísticas cada 30 segundos
        if current_time - last_stats_time >= 30:
            print_stats()
            last_stats_time = current_time

except KeyboardInterrupt:
    print("\n\n⚠️  Interrupción detectada. Mostrando estadísticas finales...\n")
    print_stats()
    print("👋 Consumer detenido.")
except Exception as e:
    print(f"\n❌ Error fatal: {e}")
    print_stats()
finally:
    consumer.close()
    client.close()