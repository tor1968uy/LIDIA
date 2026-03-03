import subprocess
import os

# ==============================================================================
# CONFIGURACIÓN PRINCIPAL
# ==============================================================================
# 1. RUTA EXACTA A mongosh.exe
raw_path = r"C:\Users\%USERNAME%\Downloads\mongosh-2.5.9-win32-x64\mongosh-2.5.9-win32-x64\bin\mongosh.exe"
MONGOSH_PATH = os.path.expandvars(raw_path)

# 2. URL BASE DE TU CLÚSTER DE ATLAS
ATLAS_URI_TEMPLATE = "mongodb+srv://{username}:{password}@mongocluster0000.msvqb0z.mongodb.net/{db}?authSource=admin"

# ==============================================================================
# CONFIGURACIÓN DE LIMPIEZA AUTOMÁTICA
# Esto rellena con las credenciales de administrador para que el script pueda
# borrar los datos de prueba antes de cada ejecución.
# ==============================================================================
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "admin"

# ==============================================================================
# LISTA DE USUARIOS Y PRUEBAS
# ==============================================================================
USERS_TO_TEST = [
    {
        "username": "user_rw_universo",
        "password": "RwUniverso123!",
        "db": "UniversoDB",
        "success_test": {
            "description": "Éxito: Insertar un cometa.",
            "query": 'db.Cometas.insertOne({nombre: "Cometa de Prueba RW Python", categoria: "Artificial", periodo_anios: 150})'
        },
        "failure_test": {
            "description": "Fallo esperado: Intentar ver el estado del servidor (acción de clúster).",
            "query": 'db.adminCommand({ serverStatus: 1 })'
        }
    },
    {
        "username": "user_read_universo",
        "password": "ReadUniverso123!",
        "db": "UniversoDB",
        "success_test": {"description": "Éxito: Leer un planeta.","query": 'db.Planetas.findOne({nombre: "Planeta Class 1"}, {nombre: 1, tipo: 1})'},
        "failure_test": {"description": "Fallo esperado: Intentar borrar un planeta.","query": 'db.Planetas.deleteOne({nombre: "Planeta Class 1"})'}
    },
    {
        "username": "user_dbadmin_universo",
        "password": "DbAdminUniverso123!",
        "db": "UniversoDB",
        "success_test": {"description": "Éxito: Ver estadísticas de una colección.","query": 'db.Astronomos.stats()'},
        "failure_test": {"description": "Fallo esperado: Intentar leer datos de una colección.","query": 'db.Astronomos.findOne({nombre: "Holly Johnson"})'}
    },
    {
        "username": "user_read_any",
        "password": "ReadAny123!",
        "db": "",
        "success_test": {"description": "Éxito: Leer de UniversoDB.","query": 'db.getSiblingDB("UniversoDB").Galaxias.countDocuments()'},
        "failure_test": {"description": "Fallo esperado: Intentar escribir en UniversoDB.","query": 'db.getSiblingDB("UniversoDB").Galaxias.insertOne({nombre: "Galaxia Ilegal Python"})'}
    },
    {
        "username": "user_cluster_monitor",
        "password": "ClusterMon123!",
        "db": "",
        "success_test": {"description": "Éxito: Ejecutar un comando de monitoreo (serverStatus).","query": 'db.adminCommand({ serverStatus: 1 })'},
        "failure_test": {"description": "Fallo esperado: Intentar leer datos de una colección.","query": 'db.getSiblingDB("UniversoDB").Galaxias.findOne()'}
    },
    {
        "username": "user_read_galaxias",
        "password": "ReadGalaxias123!",
        "db": "UniversoDB",
        "success_test": {"description": "Éxito: Leer de la colección Galaxias.","query": 'db.Galaxias.findOne({nombre: "Galaxia Lead 1"}, {nombre: 1, tipo_hubble: 1})'},
        "failure_test": {"description": "Fallo esperado: Intentar leer de la colección Planetas.","query": 'db.Planetas.findOne()'}
    },
    {
        "username": "user_rw_planetas",
        "password": "RwPlanetas123!",
        "db": "UniversoDB",
        "success_test": {"description": "Éxito: Actualizar un documento en Planetas.","query": 'db.Planetas.updateOne({nombre: "Planeta Learn 6"}, {$set: {tipo: "Rocoso"}})'},
        "failure_test": {"description": "Fallo esperado: Intentar borrar la colección Planetas.","query": 'db.Planetas.drop()'}
    },
    {
        "username": "user_geo_observatorios",
        "password": "GeoObserv123!",
        "db": "UniversoDB",
        "success_test": {"description": "Éxito: Realizar una consulta geoespacial.","query": 'db.Observatorios.find({ ubicacion: { $near: { $geometry: { type: "Point", coordinates: [-99.28, 77.90] }, $maxDistance: 1000000}}}).limit(1).toArray()'},
        "failure_test": {"description": "Fallo esperado: Intentar insertar un nuevo observatorio.","query": 'db.Observatorios.insertOne({nombre: "Nuevo Observatorio Ilegal Python"})'}
    },
    {
        "username": "user_eventos_ingesta",
        "password": "EventosIngesta123!",
        "db": "UniversoDB",
        "success_test": {"description": "Éxito: Insertar un nuevo evento.","query": 'db.EventosAstronomicos.insertOne({nombre: "Ingesta de Prueba Python", tipo: "Supernova", fecha_evento: new Date()})'},
        "failure_test": {"description": "Fallo esperado: Intentar eliminar un evento.","query": 'db.EventosAstronomicos.deleteOne({nombre: "Evento Appear 1"})'}
    },
    {
        "username": "user_imagenes_viewer",
        "password": "ImgViewer123!",
        "db": "UniversoDB",
        "success_test": {"description": "Éxito: Leer metadatos de archivos en GridFS.","query": 'db.getCollection("ImagenesGridFS.files").findOne()'},
        "failure_test": {"description": "Fallo esperado: Intentar leer de la colección Astronomos.","query": 'db.Astronomos.findOne()'}
    }
]

# ==============================================================================
# SCRIPT DE EJECUCIÓN
# ==============================================================================

def run_cleanup():
    """Ejecuta la limpieza de documentos de prueba con credenciales de admin."""
    if ADMIN_PASSWORD == "TU_CONTRASEÑA_ADMIN":
        print("ADVERTENCIA: La contraseña de administrador no ha sido configurada en el script.")
        print("Saltando la limpieza automática.\n")
        return
        
    print("--- Revirtiendo cambios de datos de pruebas anteriores ---")
    revert_script = """
    use UniversoDB;
    db.Cometas.deleteMany({ nombre: /Cometa de Prueba/ });
    db.EventosAstronomicos.deleteMany({ nombre: /Ingesta de Prueba/ });
    db.Galaxias.deleteMany({ nombre: /Galaxia Ilegal/ });
    print('Documentos de prueba anteriores eliminados.');
    """
    revert_uri = ATLAS_URI_TEMPLATE.format(username=ADMIN_USERNAME, password=ADMIN_PASSWORD, db="admin")
    revert_command = [ MONGOSH_PATH, revert_uri, "--eval", revert_script ]
    
    # Ejecutar sin mostrar la salida para mantener limpia la consola
    subprocess.run(revert_command, capture_output=True)
    print("--- Reversión completada ---\n")

def run_test(user_info):
    """Construye y ejecuta el comando mongosh para un usuario."""
    
    print("=" * 60)
    print(f"|  PROBANDO USUARIO: {user_info['username']}".ljust(59) + "|")
    print("=" * 60)

    uri = ATLAS_URI_TEMPLATE.format(
        username=user_info['username'],
        password=user_info['password'],
        db=user_info['db']
    )

    script_to_run = f"""
    print('--- Probando: {user_info["success_test"]["description"]}');
    try {{
        const result = {user_info["success_test"]["query"]};
        printjson(result);
        print('--> ÉXITO: El comando se ejecutó como se esperaba.\\n');
    }} catch (e) {{
        print('--> ERROR INESPERADO: El comando permitido falló.');
        print(e);
    }}
    
    print('--- Probando: {user_info["failure_test"]["description"]}');
    try {{
        const result = {user_info["failure_test"]["query"]};
        if (result && result.acknowledged) {{
             print('--> ERROR: El comando prohibido SE EJECUTÓ CORRECTAMENTE (fallo en la seguridad).');
        }} else {{
            // Si el comando no devuelve nada pero no da error, también es un fallo de seguridad
            print('--> ERROR: El comando prohibido PARECE HABERSE EJECUTADO (fallo en la seguridad).');
        }}
    }} catch (e) {{
        print(e);
        print('--> ÉXITO: El comando fue denegado como se esperaba.');
    }}
    """

    command = [ MONGOSH_PATH, uri, "--eval", script_to_run ]
    result = subprocess.run(command, capture_output=True, text=True, encoding='utf-8')

    print("\n--- SALIDA DE MONGOSH ---\n")
    print(result.stdout)
    if result.stderr:
        print("\n--- ERRORES DE MONGOSH ---\n")
        print(result.stderr)
    print("\n")

if __name__ == "__main__":
    if not os.path.exists(MONGOSH_PATH):
        print(f"ERROR: No se encontró 'mongosh.exe' en la ruta especificada.")
        print(f"Ruta configurada: {MONGOSH_PATH}")
    else:
        run_cleanup()
        for user in USERS_TO_TEST:
            run_test(user)
        print("=" * 60)
        print("|  Todas las pruebas han finalizado.".ljust(59) + "|")
        print("=" * 60)