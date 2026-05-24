import os
import logging

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Carpetas
BACKUP_DIR = os.path.join(BASE_DIR, "backups", "cassandra")
LOG_DIR = os.path.join(BASE_DIR, "logs")
LOG_FILE = os.path.join(LOG_DIR, "cassandra_restore.log")

# Crear carpeta logs
os.makedirs(LOG_DIR, exist_ok=True)

# Logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Configuración
CONTAINER = "cassandra-node1" # Cambiar nombre
KEYSPACE = "mathews_coffee_delivery"


def listar_backups():
    backups = os.listdir(BACKUP_DIR)
    backups.sort(reverse=True)
    return backups

# Función principal para restaurar backup
def restaurar_backup():
    try:
        logging.info("Iniciando restauración Cassandra")

        backups = listar_backups()

        if not backups:
            print("No hay backups disponibles")
            return

        print("\nBackups disponibles:\n")

        for i, backup in enumerate(backups):
            print(f"{i + 1}. {backup}")

        opcion = int(input("\nSelecciona el backup a restaurar: "))

        selected_backup = backups[opcion - 1]

        backup_host_path = os.path.join(BACKUP_DIR, selected_backup)
        container_restore_path = f"/tmp/{selected_backup}"

        print("\nCopiando backup al contenedor...")

        copy_cmd = (
            f'docker cp "{backup_host_path}" '
            f'{CONTAINER}:{container_restore_path}'
        )

        os.system(copy_cmd)

        logging.info(f"Backup copiado: {selected_backup}")

        print("Restaurando SSTables...")

        restore_cmd = (
            f"docker exec {CONTAINER} bash -c "
            f"'cp -r {container_restore_path}/* "
            f"/var/lib/cassandra/data/{KEYSPACE}/'"
        )

        os.system(restore_cmd)

        # Refrescar Cassandra
        refresh_cmd = (
            f"docker exec {CONTAINER} "
            f"nodetool refresh {KEYSPACE}"
        )

        os.system(refresh_cmd)

        logging.info("Restauración completada")

        print("\nRestauración Cassandra completada")

    except Exception as e:
        logging.error(f"Error restaurando Cassandra: {e}")
        print(f"Error: {e}")


if __name__ == "__main__":
    restaurar_backup()