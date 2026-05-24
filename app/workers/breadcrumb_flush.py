import asyncio
import logging
import os
import uuid
from datetime import datetime, timezone

from app.cassandra import cassandra_execute

logger = logging.getLogger(__name__)

FLUSH_INTERVAL = 30  # segundos entre cada ciclo de flush


# ── Core flush logic ──────────────────────────────────────────────────────────

async def flush_driver(driver_id: str, redis, cassandra) -> None:
    """
    Lee todos los entries del Stream del driver y los inserta en Cassandra.
    Solo hace XTRIM si TODOS los inserts fueron exitosos.
    Si falla alguno, los entries quedan en el Stream para el próximo ciclo.
    """
    entries = await redis.xrange(f"driver:{driver_id}:breadcrumbs")
    if not entries:
        return

    try:
        for _entry_id, fields in entries:
            ts_unix = int(fields["ts"])
            dt = datetime.fromtimestamp(ts_unix, tz=timezone.utc)
            day = dt.date()
            order_id = int(fields["order_id"])

            # ── gps_by_driver ─────────────────────────────────────────────────
            await cassandra_execute(
                cassandra,
                """
                INSERT INTO gps_by_driver (driver_id, day, ts, lat, lng, heading, speed, order_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    driver_id,
                    day,
                    dt,
                    float(fields["lat"]),
                    float(fields["lng"]),
                    int(fields["heading"]),
                    float(fields["speed"]),
                    order_id,
                ),
            )

            # ── gps_by_order ──────────────────────────────────────────────────
            await cassandra_execute(
                cassandra,
                """
                INSERT INTO gps_by_order (order_id, ts, driver_id, lat, lng, heading, speed)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    order_id,
                    dt,
                    driver_id,
                    float(fields["lat"]),
                    float(fields["lng"]),
                    int(fields["heading"]),
                    float(fields["speed"]),
                ),
            )

        # Solo trimear si todos los inserts fueron exitosos
        await redis.xtrim(f"driver:{driver_id}:breadcrumbs", maxlen=0)
        logger.info("Flushed %d breadcrumbs for driver %s", len(entries), driver_id)

    except Exception as e:
        # No trimear — los entries se preservan para el próximo ciclo
        logger.error(
            "Flush failed for driver %s (%d entries preserved): %s",
            driver_id, len(entries), e,
        )


# ── Background task ───────────────────────────────────────────────────────────

async def start_flush_worker(app) -> None:
    """
    Loop que corre cada FLUSH_INTERVAL segundos.
    Itera sobre todos los drivers en available_drivers y flushea sus breadcrumbs.
    Iniciado como asyncio.create_task() en el lifespan.
    """
    logger.info("Breadcrumb flush worker started (interval=%ds)", FLUSH_INTERVAL)
    # Ejecutar la primera iteración inmediatamente (no esperar el primer sleep)
    while True:
        try:
            # 1) Drivers explícitos marcados como available
            driver_ids = set(await app.state.redis.smembers("available_drivers") or [])

            # 2) Fallback: descubrir streams pendientes via SCAN de keys driver:*:breadcrumbs
            try:
                async for key in app.state.redis.scan_iter(match="driver:*:breadcrumbs"):
                    # key esperado: "driver:{driver_id}:breadcrumbs"
                    try:
                        parts = key.split(":")
                        if len(parts) >= 3:
                            driver_ids.add(parts[1])
                    except Exception:
                        # si el formato es inesperado, saltarlo
                        continue
            except Exception as scan_err:
                logger.debug("Redis SCAN error (continuing): %s", scan_err)

            if not driver_ids:
                # nada por procesar; dormir y seguir
                await asyncio.sleep(FLUSH_INTERVAL)
                continue

            logger.debug("Flush worker discovered %d drivers to check", len(driver_ids))

            for driver_id in list(driver_ids):
                lock_key = f"lock:flush:driver:{driver_id}"
                token = f"{os.getpid()}-{uuid.uuid4().hex}"
                try:
                    # Verificar si el stream tiene entries para evitar trabajo innecesario
                    stream_key = f"driver:{driver_id}:breadcrumbs"
                    try:
                        length = await app.state.redis.xlen(stream_key)
                    except Exception:
                        # si el comando no está disponible, fallback a XRANGE con count=1
                        entries = await app.state.redis.xrange(stream_key, count=1)
                        length = len(entries)

                    if length == 0:
                        continue

                    # Intentar adquirir lock antes de procesar para evitar que
                    # múltiples réplicas procesen el mismo stream simultáneamente.
                    lock_ttl_ms = max(10000, FLUSH_INTERVAL * 1000 * 2)
                    acquired = await app.state.redis.set(lock_key, token, nx=True, px=lock_ttl_ms)
                    if not acquired:
                        # Otro worker ya está procesando este driver
                        logger.debug("Skipping driver %s — lock held by another worker", driver_id)
                        continue

                    # Procesar el driver (tenemos el lock)
                    await flush_driver(driver_id, app.state.redis, app.state.cassandra)

                except Exception as e:
                    logger.error("Error flushing driver %s: %s", driver_id, e)
                finally:
                    # Liberar lock únicamente si aún lo poseemos (compare-and-del)
                    try:
                        # Script seguro para liberar solo si el token coincide
                        release_script = (
                            "if redis.call('get', KEYS[1]) == ARGV[1] then "
                            "return redis.call('del', KEYS[1]) else return 0 end"
                        )
                        await app.state.redis.eval(release_script, 1, lock_key, token)
                    except Exception:
                        # No fatal — si no se pudo liberar el lock, expirará por TTL
                        logger.debug("Failed to release lock for driver %s (will expire)", driver_id)

            # Esperar al final del ciclo
            await asyncio.sleep(FLUSH_INTERVAL)

        except asyncio.CancelledError:
            logger.info("Breadcrumb flush worker stopped")
            raise
        except Exception as e:
            logger.error("Flush worker cycle error: %s", e)
