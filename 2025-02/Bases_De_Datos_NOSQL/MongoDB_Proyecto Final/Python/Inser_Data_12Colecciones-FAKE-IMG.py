# import pymongo
# import gridfs
# from faker import Faker
# import random
# from datetime import datetime, timedelta
# import io
#
# # --- Conexión y Configuración ---
# MONGO_URI = "mongodb://localhost:27018/"
# DB_NAME = "UniversoDB"
# client = pymongo.MongoClient(MONGO_URI)
# db = client[DB_NAME]
# fs = gridfs.GridFS(db, collection="ImagenesGridFS")
# fake = Faker()
#
# # --- Parámetros de Carga ---
# NUM_AGENCIAS = 50
# NUM_OBSERVATORIOS = 100
# NUM_ASTRONOMOS = 1000
# NUM_MISIONES = 500
# NUM_SATELITES_ARTIFICIALES = 50000
# NUM_GALAXIAS = 2000
# NUM_ESTRELLAS = 30000
# NUM_PLANETAS = 50000
# NUM_SATELITES_NATURALES = 15000
# NUM_COMETAS = 5000
# NUM_EVENTOS = 10000
# NUM_IMAGENES = 1000
#
#
# # --- 1. Definición de Validadores JSON Schema ---
# def setup_validation(db):
#     """Aplica validadores JSON Schema a 3 colecciones."""
#     print("Aplicando validadores JSON Schema...")
#
#     # Validador 1: Observatorios (GeoJSON)
#     observatorio_validator = {
#         '$jsonSchema': {
#             'bsonType': 'object',
#             'required': ['nombre', 'pais', 'ubicacion'],
#             'properties': {
#                 'nombre': {'bsonType': 'string', 'description': 'Debe ser un string'},
#                 'pais': {'bsonType': 'string'},
#                 'ubicacion': {
#                     'bsonType': 'object',
#                     'required': ['type', 'coordinates'],
#                     'properties': {
#                         'type': {'enum': ['Point'], 'description': 'Solo se permite tipo Punto'},
#                         'coordinates': {
#                             'bsonType': 'array',
#                             'minItems': 2,
#                             'maxItems': 2,
#                             'items': {'bsonType': 'double'}
#                         }
#                     }
#                 }
#             }
#         }
#     }
#
#     # Validador 2: Planetas (Documento Anidado y Referencia)
#     planeta_validator = {
#         '$jsonSchema': {
#             'bsonType': 'object',
#             'required': ['nombre', 'tipo', 'estrella_id'],
#             'properties': {
#                 'nombre': {'bsonType': 'string'},
#                 'tipo': {'enum': ['Rocoso', 'Gaseoso', 'Enano', 'Exoplaneta']},
#                 'estrella_id': {'bsonType': 'objectId', 'description': 'Debe ser una referencia ObjectId a Estrellas'},
#                 'masa_kg': {'bsonType': 'double', 'minimum': 0},
#                 'atmosfera': {
#                     'bsonType': 'object',
#                     'properties': {
#                         'componentes': {'bsonType': 'array', 'items': {'bsonType': 'string'}},
#                         'presion_bar': {'bsonType': 'double'}
#                     }
#                 }
#             }
#         }
#     }
#
#     # Validador 3: Astronomos (Arrays y Tipos)
#     astronomo_validator = {
#         '$jsonSchema': {
#             'bsonType': 'object',
#             'required': ['nombre', 'nacionalidad', 'fecha_nacimiento'],
#             'properties': {
#                 'nombre': {'bsonType': 'string'},
#                 'nacionalidad': {'bsonType': 'string'},
#                 'fecha_nacimiento': {'bsonType': 'date'},
#                 'descubrimientos_ids': {
#                     'bsonType': 'array',
#                     'items': {'bsonType': 'objectId'}
#                 }
#             }
#         }
#     }
#
#     # Aplicar validadores (usando collMod)
#     try:
#         db.command('collMod', 'Observatorios', validator=observatorio_validator)
#     except pymongo.errors.OperationFailure:
#         db.create_collection('Observatorios', validator=observatorio_validator)
#
#     try:
#         db.command('collMod', 'Planetas', validator=planeta_validator)
#     except pymongo.errors.OperationFailure:
#         db.create_collection('Planetas', validator=planeta_validator)
#
#     try:
#         db.command('collMod', 'Astronomos', validator=astronomo_validator)
#     except pymongo.errors.OperationFailure:
#         db.create_collection('Astronomos', validator=astronomo_validator)
#
#     print("Validadores aplicados.")
#
#
# # --- 2. Funciones de Generación de Datos ---
#
# def generate_and_store_image(filename):
#     """Crea datos binarios falsos y los guarda en GridFS."""
#     fake_image_data = b"Este es un archivo de imagen falso para " + bytes(filename, 'utf-8') + b" " * random.randint(10,
#                                                                                                                      100)
#     file_id = fs.put(io.BytesIO(fake_image_data), filename=filename, contentType="image/fake")
#     return file_id
#
#
# # 1. Agencias Espaciales
# def generate_agencias(num):
#     print(f"Generando {num} Agencias...")
#     agencias = []
#     for i in range(num):
#         agencias.append({
#             "nombre": f"{fake.company()} {i + 1}",
#             "pais_sede": fake.country(),
#             "anio_fundacion": random.randint(1950, 2020)
#         })
#     result = db["AgenciasEspaciales"].insert_many(agencias)
#     return result.inserted_ids
#
#
# # 2. Observatorios (GeoJSON)
# def generate_observatorios(num):
#     print(f"Generando {num} Observatorios...")
#     observatorios = []
#     for i in range(num):
#         observatorios.append({
#             "nombre": f"{fake.company()} Observatory {i + 1}",
#             "pais": fake.country(),
#             "ubicacion": {  # CAMPO GEOJSON
#                 "type": "Point",
#                 "coordinates": [float(fake.longitude()), float(fake.latitude())]
#             }
#         })
#     result = db["Observatorios"].insert_many(observatorios)
#     # Crear índice geoespacial
#     db["Observatorios"].create_index([("ubicacion", pymongo.GEOSPHERE)])
#     return result.inserted_ids
#
#
# # 3. Astronomos
# def generate_astronomos(num, observatorio_ids):
#     print(f"Generando {num} Astronomos...")
#     astronomos = []
#     for _ in range(num):
#         astronomos.append({
#             "nombre": fake.name(),
#             "nacionalidad": fake.country(),
#             "fecha_nacimiento": fake.date_time_between(start_date='-70y', end_date='-30y'),
#             "observatorio_id": random.choice(observatorio_ids),  # REFERENCIA
#             "descubrimientos_ids": []  # Se llenará después (p.ej. Cometas)
#         })
#     result = db["Astronomos"].insert_many(astronomos)
#     return result.inserted_ids
#
#
# # 4. Misiones (Array de Referencias)
# def generate_misiones(num, agencia_ids, astronomo_ids):
#     print(f"Generando {num} Misiones...")
#     misiones = []
#     for i in range(num):
#         misiones.append({
#             "nombre": f"{fake.word().capitalize()} {i + 1}",
#             "agencia_id": random.choice(agencia_ids),  # REFERENCIA
#             "fecha_lanzamiento": fake.date_time_this_decade(),
#             "estado": random.choice(["Planeada", "Activa", "Completada"]),
#             "astronautas_ids": random.sample(astronomo_ids, k=random.randint(1, 5))  # ARRAY DE REFERENCIAS
#         })
#     result = db["MisionesEspaciales"].insert_many(misiones)
#     return result.inserted_ids
#
#
# # 5. Satelites Artificiales
# def generate_satelites_artificiales(num, mision_ids):
#     print(f"Generando {num} Satélites Artificiales...")
#     satelites = []
#     for i in range(num):
#         satelites.append({
#             "nombre": f"{fake.word().upper()}-{i + 1}",
#             "mision_id": random.choice(mision_ids),  # REFERENCIA
#             "tipo": random.choice(["Comunicación", "Observación", "GPS", "Científico"]),
#             "estado": random.choice(["Activo", "Inactivo"])
#         })
#     result = db["SatelitesArtificiales"].insert_many(satelites)
#     return result.inserted_ids
#
#
# # 6. Galaxias (con referencia a Imagen GridFS)
# def generate_galaxias(num, imagen_ids):
#     print(f"Generando {num} Galaxias...")
#     galaxias = []
#     tipos = ["Espiral", "Elíptica", "Irregular"]
#     for i in range(num):
#         galaxias.append({
#             "nombre": f"{fake.word().capitalize()} Galaxy {i + 1}",
#             "tipo_hubble": random.choice(tipos),
#             "estrellas_estimadas": random.randint(int(1e8), int(1e12)),
#             "imagen_principal_id": random.choice(imagen_ids)  # REFERENCIA A GRIDFS
#         })
#     result = db["Galaxias"].insert_many(galaxias)
#     return result.inserted_ids
#
#
# # 7. Estrellas (Documento Anidado)
# def generate_estrellas(num, galaxia_ids):
#     print(f"Generando {num} Estrellas...")
#     estrellas = []
#     tipos_espectrales = ["O", "B", "A", "F", "G", "K", "M"]
#     for i in range(num):
#         estrellas.append({
#             "nombre": f"{fake.word().capitalize()} {i + 1}",
#             "galaxia_id": random.choice(galaxia_ids),  # REFERENCIA
#             "propiedades_fisicas": {  # DOCUMENTO ANIDADO
#                 "tipo_espectral": random.choice(tipos_espectrales),
#                 "luminosidad_solar": random.uniform(0.001, 100000),
#                 "temperatura_k": random.randint(2000, 50000)
#             }
#         })
#     result = db["Estrellas"].insert_many(estrellas)
#     return result.inserted_ids
#
#
# # 8. Planetas (Documento Anidado y Referencia)
# def generate_planetas(num, estrella_ids, imagen_ids):
#     print(f"Generando {num} Planetas...")
#     planetas = []
#     tipos = ["Rocoso", "Gaseoso", "Enano", "Exoplaneta"]
#     for i in range(num):
#         planetas.append({
#             "nombre": f"{fake.word().capitalize()} {i + 1}",
#             "tipo": random.choice(tipos),
#
#             # --- ¡AQUÍ ESTÁ LA LÓGICA! ---
#             # Cada planeta que se crea recibe un _id de estrella aleatorio
#             # de la lista de estrellas que ya existen.
#             "estrella_id": random.choice(estrella_ids),  # REFERENCIA
#
#             "masa_kg": random.uniform(1e20, 1e27),
#             "atmosfera": {  # DOCUMENTO ANIDADO
#                 "componentes": random.sample(["Metano", "Hidrógeno", "Helio", "Oxígeno"], k=random.randint(1, 3)),
#                 "presion_bar": random.uniform(0.01, 100)
#             },
#             "imagen_id": random.choice(imagen_ids)
#         })
#     result = db["Planetas"].insert_many(planetas)
#     return result.inserted_ids
#
#
# # 9. Satelites Naturales (Lunas)
# def generate_satelites_naturales(num, planeta_ids):
#     print(f"Generando {num} Satélites Naturales...")
#     satelites = []
#     for i in range(num):
#         satelites.append({
#             "nombre": f"Luna {fake.word().capitalize()} {i + 1}",
#             "planeta_id": random.choice(planeta_ids),  # REFERENCIA
#             "radio_km": random.uniform(10, 5000),
#         })
#     result = db["SatelitesNaturales"].insert_many(satelites)
#     return result.inserted_ids
#
#
# # 10. Cometas
# def generate_cometas(num, astronomo_ids):
#     print(f"Generando {num} Cometas...")
#     cometas_docs = []
#
#     # 1. Preparar los documentos de cometas
#     for i in range(num):
#         descubridor_id = random.choice(astronomo_ids)
#         cometa = {
#             "nombre": f"Cometa {fake.word().capitalize()} {i + 1}",
#             "periodo_anios": random.randint(1, 1000),
#             "descubridor_id": descubridor_id,  # REFERENCIA
#         }
#         cometas_docs.append(cometa)
#
#     # 2. Ejecutar el PRIMER Bulk: Insertar todos los cometas
#     print("  -> (Bulk 1/2) Insertando cometas...")
#     bulk_ops_cometas = [pymongo.InsertOne(c) for c in cometas_docs]
#     result_cometas = db["Cometas"].bulk_write(bulk_ops_cometas)
#
#     # --- ¡LÍNEA CORREGIDA! ---
#     # Extraemos los IDs de la lista original 'cometas_docs',
#     # que pymongo modificó internamente al añadir los '_id'
#     inserted_comet_ids = [doc['_id'] for doc in cometas_docs]
#
#     # 3. Preparar el SEGUNDO Bulk: Actualizar los astrónomos
#     # Ahora 'inserted_comet_ids' y 'cometas_docs' están en el mismo orden
#     bulk_ops_astronomos = []
#     for i, cometa_doc in enumerate(cometas_docs):
#         descubridor_id = cometa_doc['descubridor_id']
#         nuevo_cometa_id = inserted_comet_ids[i]  # <-- ¡Ahora sí tenemos el _id!
#
#         # Añadir una operación de actualización a la lista
#         bulk_ops_astronomos.append(
#             pymongo.UpdateOne(
#                 {'_id': descubridor_id},
#                 {'$push': {'descubrimientos_ids': nuevo_cometa_id}}
#             )
#         )
#
#     # 4. Ejecutar el SEGUNDO Bulk: Actualizar todos los astrónomos
#     print("  -> (Bulk 2/2) Actualizando astrónomos con descubrimientos...")
#     if bulk_ops_astronomos:
#         db["Astronomos"].bulk_write(bulk_ops_astronomos)
#
#     return inserted_comet_ids
#
#
# # 11. Eventos Astronomicos (Array de Referencias)
# def generate_eventos(num, observatorio_ids, estrella_ids, galaxia_ids, imagen_ids):
#     print(f"Generando {num} Eventos...")
#     eventos = []
#     tipos = ["Supernova", "Eclipse", "Lluvia de Meteoros", "Conjunción"]
#     for i in range(num):
#         eventos.append({
#             "nombre": f"Evento {fake.word().capitalize()} {i + 1}",
#             "tipo": random.choice(tipos),
#             "fecha_evento": fake.date_time_this_decade(),
#             "observatorio_registrador_id": random.choice(observatorio_ids),  # REFERENCIA
#             "objetos_relacionados_ids": [  # ARRAY DE REFERENCIAS
#                 random.choice(estrella_ids),
#                 random.choice(galaxia_ids)
#             ],
#             "imagen_evento_id": random.choice(imagen_ids)  # REFERENCIA A GRIDFS
#         })
#     result = db["EventosAstronomicos"].insert_many(eventos)
#     return result.inserted_ids
#
#
# # 12. Imágenes (GridFS)
# def generate_imagenes(num):
#     print(f"Generando {num} Imágenes en GridFS...")
#     imagen_ids = []
#     for i in range(num):
#         filename = f"imagen_espacial_{i + 1}.fake"
#         file_id = generate_and_store_image(filename)
#         imagen_ids.append(file_id)
#     print(f"Almacenadas {len(imagen_ids)} imágenes en GridFS.")
#     return imagen_ids
#
#
# # --- 3. Script Principal de Inserción ---
#
# def insert_data():
#     """Limpia la BD y ejecuta todas las funciones de generación en orden."""
#     print(f"Borrando la base de datos {DB_NAME}...")
#
#     # 1. Aplicar validadores a colecciones vacías
#     setup_validation(db)
#
#     # 2. Generar datos en orden de dependencia
#
#     # Entidades independientes o de bajo nivel
#     print("--- Fase 1: Entidades Base ---")
#     imagen_ids = generate_imagenes(NUM_IMAGENES)  # 12. GridFS
#     agencia_ids = generate_agencias(NUM_AGENCIAS)  # 1.
#     observatorio_ids = generate_observatorios(NUM_OBSERVATORIOS)  # 2.
#     astronomo_ids = generate_astronomos(NUM_ASTRONOMOS, observatorio_ids)  # 3.
#     galaxia_ids = generate_galaxias(NUM_GALAXIAS, imagen_ids)  # 6.
#
#     # Entidades dependientes
#     print("\n--- Fase 2: Entidades Dependientes ---")
#     mision_ids = generate_misiones(NUM_MISIONES, agencia_ids, astronomo_ids)  # 4.
#     estrella_ids = generate_estrellas(NUM_ESTRELLAS, galaxia_ids)  # 7.
#     planeta_ids = generate_planetas(NUM_PLANETAS, estrella_ids, imagen_ids)  # 8.
#
#     # Entidades de alto nivel y vinculación
#     print("\n--- Fase 3: Entidades de Vinculación ---")
#     satelite_art_ids = generate_satelites_artificiales(NUM_SATELITES_ARTIFICIALES, mision_ids)  # 5.
#     satelite_nat_ids = generate_satelites_naturales(NUM_SATELITES_NATURALES, planeta_ids)  # 9.
#     cometa_ids = generate_cometas(NUM_COMETAS, astronomo_ids)  # 10.
#     evento_ids = generate_eventos(NUM_EVENTOS, observatorio_ids, estrella_ids, galaxia_ids, imagen_ids)  # 11.
#
#     print("\n--- ¡Carga Masiva Completada! ---")
#     print(f"Se crearon 11 colecciones + 2 colecciones de GridFS (ImagenesGridFS.files, ImagenesGridFS.chunks)")
#
#     client.close()
#
#
# if __name__ == "__main__":
#     insert_data()

import pymongo
import gridfs
from faker import Faker
import random
from datetime import datetime
import os
import io

# --- Conexión y Configuración ---
MONGO_URI = "mongodb://admin:admin123@localhost:27018/UniversoDB?tls=true&tlsCAFile=../certs/ca.crt&tlsAllowInvalidHostnames=true&authSource=admin"

DB_NAME = "UniversoDB"
client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]
fs = gridfs.GridFS(db, collection="ImagenesGridFS")
fake = Faker()

# --- Carpeta de imágenes reales ---
IMG_FOLDER = os.path.join(os.getcwd(), "IMG")

# --- Parámetros de Carga ---
NUM_AGENCIAS = 100
NUM_OBSERVATORIOS = 500
NUM_ASTRONOMOS = 500
NUM_MISIONES = 2000
NUM_SATELITES_ARTIFICIALES = 100000
NUM_GALAXIAS = 2000
NUM_ESTRELLAS = 80000
NUM_PLANETAS = 12000
NUM_SATELITES_NATURALES = 400
NUM_COMETAS = 200
NUM_EVENTOS = 30000


# --- Funciones auxiliares ---
def generar_masa():
    return round(random.uniform(1e15, 1e30), 3)

def generar_gravedad():
    return round(random.uniform(0.1, 300), 3)

def fecha_aleatoria_pasada():
    return fake.date_time_between(start_date='-70y', end_date='-10y')

def fecha_aleatoria_reciente():
    return fake.date_time_between(start_date='-10y', end_date='now')


# --- 1. Cargar imágenes reales desde carpeta ---
#def generate_imagenes_desde_carpeta():
    # print(f"📁 Cargando imágenes desde {IMG_FOLDER} ...")
    # imagen_ids = []
    # archivos = sorted(os.listdir(IMG_FOLDER))
    # for archivo in archivos:
    #     ruta = os.path.join(IMG_FOLDER, archivo)
    #     if os.path.isfile(ruta):
    #         with open(ruta, "rb") as f:
    #             file_id = fs.put(f, filename=archivo, contentType="image/jpeg")
    #             imagen_ids.append(file_id)
    # print(f"✅ Se cargaron {len(imagen_ids)} imágenes reales en GridFS.")
    # return imagen_ids

# def generate_and_store_image(filename):
#      """Crea datos binarios falsos y los guarda en GridFS."""
#      fake_image_data = b"Este es un archivo de imagen falso para " + bytes(filename, 'utf-8') + b" " * random.randint(10,
#                                                                                                                       100)
#      file_id = fs.put(io.BytesIO(fake_image_data), filename=filename, contentType="image/fake")
#      return file_id
def generate_imagenes_desde_carpeta():
    print("🧪 Generando imágenes falsas en GridFS...")
    imagen_ids = []
    for i in range(50):  # genera 50 imágenes simuladas
        filename = f"imagen_fake_{i+1}.fake"
        fake_image_data = b"Este es un archivo de imagen falso para " + bytes(filename, 'utf-8') + b" " * random.randint(10, 100)
        file_id = fs.put(io.BytesIO(fake_image_data), filename=filename, contentType="image/fake")
        imagen_ids.append(file_id)
    print(f"✅ Se generaron {len(imagen_ids)} imágenes falsas en GridFS.")
    return imagen_ids

# --- 2. Generadores de datos ---
def generate_agencias(num):
    print(f"Generando {num} agencias espaciales...")
    docs = []
    for i in range(num):
        docs.append({
            "nombre": f"{fake.company()} Space Agency {i+1}",
            "pais_sede": fake.country(),
            "anio_fundacion": random.randint(1950, 2020),
            "categoria": "Artificial"
        })
    return db["AgenciasEspaciales"].insert_many(docs).inserted_ids


def generate_observatorios(num):
    print(f"Generando {num} observatorios...")
    docs = []
    for i in range(num):
        docs.append({
            "nombre": f"{fake.company()} Observatory {i+1}",
            "pais": fake.country(),
            "ubicacion": {
                "type": "Point",
                "coordinates": [float(fake.longitude()), float(fake.latitude())]
            },
            "categoria": "Artificial"
        })
    #db["Observatorios"].create_index([("ubicacion", pymongo.GEOSPHERE)])
    return db["Observatorios"].insert_many(docs).inserted_ids


def generate_astronomos(num, observatorio_ids):
    print(f"Generando {num} astrónomos...")
    docs = []
    for i in range(num):
        docs.append({
            "nombre": fake.name(),
            "nacionalidad": fake.country(),
            "fecha_nacimiento": fake.date_time_between(start_date='-70y', end_date='-30y'),
            "observatorio_id": random.choice(observatorio_ids),
            "categoria": "Artificial"
        })
    return db["Astronomos"].insert_many(docs).inserted_ids


def generate_misiones(num, agencia_ids, astronomo_ids):
    print(f"Generando {num} misiones espaciales...")
    docs = []
    for i in range(num):
        docs.append({
            "nombre": f"Misión {fake.word().capitalize()}-{i+1}",
            "agencia_id": random.choice(agencia_ids),
            "fecha_lanzamiento": fecha_aleatoria_reciente(),
            "estado": random.choice(["Planeada", "Activa", "Completada"]),
            "astronautas_ids": random.sample(astronomo_ids, k=random.randint(1, 5)),
            "categoria": "Artificial"
        })
    return db["MisionesEspaciales"].insert_many(docs).inserted_ids


def generate_satelites_artificiales(num, mision_ids, imagen_ids):
    print(f"Generando {num} satélites artificiales...")
    docs = []
    for i in range(num):
        docs.append({
            "nombre": f"SAT-{i+1:05d}",
            "mision_id": random.choice(mision_ids),
            "tipo": random.choice(["Comunicación", "Observación", "GPS", "Científico"]),
            "masa_kg": generar_masa(),
            "gravedad_m_s2": generar_gravedad(),
            "fecha_puesta_en_orbita": fecha_aleatoria_reciente(),
            "estado": random.choice(["Activo", "Inactivo"]),
            "imagen_id": random.choice(imagen_ids),
            "categoria": "Artificial"
        })
    return db["SatelitesArtificiales"].insert_many(docs).inserted_ids


def generate_galaxias(num, imagen_ids):
    print(f"Generando {num} galaxias...")
    tipos = ["Espiral", "Elíptica", "Irregular"]
    docs = []
    for i in range(num):
        docs.append({
            "nombre": f"Galaxia {fake.word().capitalize()} {i+1}",
            "tipo_hubble": random.choice(tipos),
            "estrellas_estimadas": random.randint(int(1e8), int(1e12)),
            "masa_kg": generar_masa(),
            "gravedad_m_s2": generar_gravedad(),
            "fecha_descubrimiento": fecha_aleatoria_pasada(),
            "imagen_id": random.choice(imagen_ids),
            "categoria": "Natural"
        })
    return db["Galaxias"].insert_many(docs).inserted_ids


def generate_estrellas(num, galaxia_ids, imagen_ids):
    print(f"Generando {num} estrellas...")
    tipos_espectrales = ["O", "B", "A", "F", "G", "K", "M"]
    descripciones = ["Enana blanca", "Gigante roja", "Gigante azul", "De neutrones", "Supergigante", "Secuencia principal"]
    docs = []
    for i in range(num):
        docs.append({
            "nombre": f"Estrella {fake.word().capitalize()} {i+1}",
            "galaxia_id": random.choice(galaxia_ids),
            "propiedades_fisicas": {
                "tipo_espectral": random.choice(tipos_espectrales),
                "descripcion": random.choice(descripciones),
                "luminosidad_solar": random.uniform(0.001, 100000),
                "temperatura_k": random.randint(2000, 50000)
            },
            "masa_kg": generar_masa(),
            "gravedad_m_s2": generar_gravedad(),
            "fecha_descubrimiento": fecha_aleatoria_pasada(),
            "imagen_id": random.choice(imagen_ids),
            "categoria": "Natural"
        })
    return db["Estrellas"].insert_many(docs).inserted_ids


def generate_planetas(num, estrella_ids, imagen_ids):
    print(f"Generando {num} planetas...")
    tipos = ["Rocoso", "Gaseoso", "Enano", "Exoplaneta"]
    docs = []
    for i in range(num):
        docs.append({
            "nombre": f"Planeta {fake.word().capitalize()} {i+1}",
            "tipo": random.choice(tipos),
            "estrella_id": random.choice(estrella_ids),
            "masa_kg": generar_masa(),
            "gravedad_m_s2": generar_gravedad(),
            "fecha_descubrimiento": fecha_aleatoria_pasada(),
            "atmosfera": {
                "componentes": random.sample(["Metano", "Hidrógeno", "Helio", "Oxígeno"], k=random.randint(1, 3)),
                "presion_bar": random.uniform(0.01, 100)
            },
            "imagen_id": random.choice(imagen_ids),
            "categoria": "Natural"
        })
    return db["Planetas"].insert_many(docs).inserted_ids


def generate_satelites_naturales(num, planeta_ids, imagen_ids):
    print(f"Generando {num} satélites naturales...")
    docs = []
    for i in range(num):
        docs.append({
            "nombre": f"Luna {fake.word().capitalize()} {i+1}",
            "planeta_id": random.choice(planeta_ids),
            "radio_km": random.uniform(10, 5000),
            "masa_kg": generar_masa(),
            "gravedad_m_s2": generar_gravedad(),
            "fecha_descubrimiento": fecha_aleatoria_pasada(),
            "imagen_id": random.choice(imagen_ids),
            "categoria": "Natural"
        })
    return db["SatelitesNaturales"].insert_many(docs).inserted_ids


def generate_cometas(num, astronomo_ids, imagen_ids):
    print(f"Generando {num} cometas...")
    docs = []
    for i in range(num):
        docs.append({
            "nombre": f"Cometa {fake.word().capitalize()} {i+1}",
            "descubridor_id": random.choice(astronomo_ids),
            "periodo_anios": random.randint(1, 1000),
            "masa_kg": generar_masa(),
            "gravedad_m_s2": generar_gravedad(),
            "fecha_descubrimiento": fecha_aleatoria_pasada(),
            "imagen_id": random.choice(imagen_ids),
            "categoria": "Natural"
        })
    return db["Cometas"].insert_many(docs).inserted_ids


def generate_eventos(num, observatorio_ids, estrella_ids, galaxia_ids, imagen_ids):
    print(f"Generando {num} eventos astronómicos...")
    tipos = ["Supernova", "Eclipse", "Lluvia de Meteoros", "Conjunción"]
    docs = []
    for i in range(num):
        docs.append({
            "nombre": f"Evento {fake.word().capitalize()} {i+1}",
            "tipo": random.choice(tipos),
            "fecha_evento": fecha_aleatoria_reciente(),
            "observatorio_registrador_id": random.choice(observatorio_ids),
            "objetos_relacionados_ids": [random.choice(estrella_ids), random.choice(galaxia_ids)],
            "imagen_id": random.choice(imagen_ids),
            "categoria": "Natural"
        })
    return db["EventosAstronomicos"].insert_many(docs).inserted_ids


# --- 3. Inserción principal ---
def insert_data():
    print("🚀 Iniciando carga del catálogo astronómico (con categorías)...")
    imagen_ids = generate_imagenes_desde_carpeta()
    agencia_ids = generate_agencias(NUM_AGENCIAS)
    observatorio_ids = generate_observatorios(NUM_OBSERVATORIOS)
    astronomo_ids = generate_astronomos(NUM_ASTRONOMOS, observatorio_ids)
    galaxia_ids = generate_galaxias(NUM_GALAXIAS, imagen_ids)
    mision_ids = generate_misiones(NUM_MISIONES, agencia_ids, astronomo_ids)
    estrella_ids = generate_estrellas(NUM_ESTRELLAS, galaxia_ids, imagen_ids)
    planeta_ids = generate_planetas(NUM_PLANETAS, estrella_ids, imagen_ids)
    generate_satelites_artificiales(NUM_SATELITES_ARTIFICIALES, mision_ids, imagen_ids)
    generate_satelites_naturales(NUM_SATELITES_NATURALES, planeta_ids, imagen_ids)
    generate_cometas(NUM_COMETAS, astronomo_ids, imagen_ids)
    generate_eventos(NUM_EVENTOS, observatorio_ids, estrella_ids, galaxia_ids, imagen_ids)
    print("✅ Carga completa. UniversoDB lista con categorías Natural/Artificial.")


if __name__ == "__main__":
    insert_data()

