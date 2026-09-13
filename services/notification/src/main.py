"""Notification worker entrypoint (spec §4.7).

Consumes `loan.status.changed.v1` (key = applicant_id => order per client)
and publishes JSON on Redis Pub/Sub `loan-status:{applicant_id}`.
The gateway SSE handler subscribes there — the gateway never touches Kafka.

Retry/DLQ lands in Etap 9 via libs/kafka BaseConsumer; until then failures
are logged and the offset is NOT committed (redelivery).
"""

from __future__ import annotations

import asyncio
import signal
from contextlib import suppress

from aiokafka import AIOKafkaConsumer
from crediguard_observability import configure_logging, get_logger
from redis.asyncio import Redis

from src.config import Settings
from src.mapper import handle_message

configure_logging("notification-service")
logger = get_logger()


async def run() -> None:
    settings = Settings()
    consumer = AIOKafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    redis = Redis.from_url(settings.redis_url, decode_responses=False)
    await consumer.start()
    logger.info(
        "Notification worker started",
        topic=settings.kafka_topic,
        group_id=settings.kafka_group_id,
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
    try:
        async for record in consumer:
            if stop.is_set():
                break
            try:
                raw = record.value
                if isinstance(raw, str):
                    raw = raw.encode("utf-8")
                channel, message = handle_message(bytes(raw))
                await redis.publish(channel, message)
                await consumer.commit()
                logger.info(
                    "Bridged status event to Redis",
                    channel=channel,
                    partition=record.partition,
                    offset=record.offset,
                )
            except Exception:
                # No commit => redelivery. DLQ handling arrives in Etap 9.
                logger.exception(
                    "Failed to bridge status event",
                    partition=record.partition,
                    offset=record.offset,
                )
    finally:
        await consumer.stop()
        await redis.aclose()
        logger.info("Notification worker stopped")


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
