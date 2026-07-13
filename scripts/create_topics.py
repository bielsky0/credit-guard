#!/usr/bin/env python3
"""Creates Kafka topics + DLQs for CrediGuard (spec docs/SPECYFIKACJA.md §5.1).

Shells out to the kafka-topics.sh bundled in the running `kafka` container via
`docker compose exec`, so no Kafka client library is required on the host at
this stage. Safe to re-run (--if-not-exists).
"""

from __future__ import annotations

import subprocess
import sys

COMPOSE_FILE = "docker-compose.infra.yml"
BOOTSTRAP_SERVER = "localhost:9092"  # in-network address, as seen from inside the kafka container
KAFKA_TOPICS_SH = "/opt/kafka/bin/kafka-topics.sh"

# (topic, partitions) — replication factor is always 1 (single broker in dev).
TOPICS: list[tuple[str, int]] = [
    ("loan.application.submitted.v1", 3),
    ("document.verified.v1", 3),
    ("document.rejected.v1", 3),
    ("underwriting.completed.v1", 3),
    ("disbursement.completed.v1", 3),
    ("disbursement.failed.v1", 3),
    ("loan.status.changed.v1", 3),
]

DLQ_SUFFIX = ".dlq"
DLQ_PARTITIONS = 1


def create_topic(name: str, partitions: int) -> None:
    cmd = [
        "docker",
        "compose",
        "-f",
        COMPOSE_FILE,
        "exec",
        "-T",
        "kafka",
        KAFKA_TOPICS_SH,
        "--bootstrap-server",
        BOOTSTRAP_SERVER,
        "--create",
        "--if-not-exists",
        "--topic",
        name,
        "--partitions",
        str(partitions),
        "--replication-factor",
        "1",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"FAILED: {name}\n{result.stderr}", file=sys.stderr)
        raise SystemExit(1)
    print(f"OK  {name} (partitions={partitions})")


def main() -> None:
    for topic, partitions in TOPICS:
        create_topic(topic, partitions)
        create_topic(f"{topic}{DLQ_SUFFIX}", DLQ_PARTITIONS)


if __name__ == "__main__":
    main()
