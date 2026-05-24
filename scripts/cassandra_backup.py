import os
import logging
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Carpetas
BACKUP_DIR = os.path.join(BASE_DIR, "backups", "cassandra")
LOG_DIR = os.path.join(BASE_DIR, "logs")
LOG_FILE = os.path.join(LOG_DIR, "cassandra_backup.log")

# Crear carpetas si no existen
os.makedirs(BACKUP_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# Configuración logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Configuración Cassandra
CONTAINER = "cassandra-node1" # Cambiar nombre
KEYSPACE = "mathews_coffee_delivery"

# Timestamp
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
SNAPSHOT_NAME = f"snapshot_{timestamp}"

try:
    logging.info("Iniciando backup Cassandra")

    print("\nCreando snapshot en Cassandra")

    # Crear snapshot
    snapshot_cmd = (
        f"docker exec {CONTAINER} "
        f"nodetool snapshot -t {SNAPSHOT_NAME} {KEYSPACE}"
    )

    os.system(snapshot_cmd)

    logging.info("Snapshot creado correctamente")

    print("Copiando snapshot al host")

    # Ruta interna Cassandra
    container_snapshot_path = (
        f"/var/lib/cassandra/data/{KEYSPACE}"
    )

    # Copiar snapshots
    copy_cmd = (
        f'docker cp {CONTAINER}:{container_snapshot_path} '
        f'"{BACKUP_DIR}/{SNAPSHOT_NAME}"'
    )

    os.system(copy_cmd)

    logging.info("Backup copiado al host")

    print(f"\nBackup Cassandra creado correctamente:")
    print(f"{BACKUP_DIR}/{SNAPSHOT_NAME}")

except Exception as e:
    logging.error(f"Error en backup Cassandra: {e}")
    print(f"Error: {e}")