#!/usr/bin/env python3
"""
batch_generator.py

Genera archivos CSV de datos sintéticos para el ejercicio
“Chapter 2 – Batch Processing”.

Flujo:
1. create_data  – crea n registros ficticios.
2. write_to_csv – los escribe en CSV.
3. add_id       – añade columna unique_id (UUID-4).
4. update_datetime – normaliza el campo accessed_at a ISO-8601.

Requisitos:
    Python ≥ 3.10
    pip install faker polars
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import logging
import random
import uuid
from pathlib import Path
from typing import Dict, List

import polars as pl
from faker import Faker

fake = Faker()

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# --------------------------------------------------------------------------- #
# Generadores auxiliares
# --------------------------------------------------------------------------- #
def _rand_int(a: int, b: int) -> int:
    """Devuelve un entero aleatorio entre a y b (inclusive)."""
    return random.randint(a, b)


def generate_record() -> Dict[str, str]:
    """Genera un registro sintético (16 campos, sin unique_id aún)."""
    session_duration = _rand_int(30, 7200)  # s
    download_speed  = _rand_int(10, 150)    # Mbps
    upload_speed    = _rand_int(5, 100)     # Mbps
    consumed_traffic = round((download_speed + upload_speed)
                             * session_duration / 8, 2)  # MB aprox.

    return {
        "person_name": fake.name(),
        "user_name": fake.user_name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "address": fake.address().replace("\n", ", "),
        "mac_address": fake.mac_address(),
        "ip_address": fake.ipv4_public(),
        "iban": fake.iban(),
        "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),        "accessed_at": fake.date_time_between("-2y", "now").isoformat(timespec="seconds"),
        "session_duration": session_duration,
        "download_speed": download_speed,
        "upload_speed": upload_speed,
        "consumed_traffic": consumed_traffic,
        "personal_number": fake.random_number(10, fix_len=True),
    }


def create_data(n: int) -> List[Dict[str, str]]:
    """Crea n registros sintéticos."""
    logging.info("Creando %s registros…", n)
    return [generate_record() for _ in range(n)]


# --------------------------------------------------------------------------- #
# Persistencia
# --------------------------------------------------------------------------- #
def write_to_csv(records: List[Dict[str, str]], out_dir: Path,
                 fname: str | None = None) -> Path:
    """Escribe records en CSV y devuelve la ruta resultante."""
    out_dir.mkdir(parents=True, exist_ok=True)
    if fname is None:
        ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = f"batch_{ts}.csv"

    csv_path = out_dir / fname
    logging.info("Escribiendo archivo: %s", csv_path)

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(records[0].keys()))
        writer.writeheader()
        writer.writerows(records)

    return csv_path


def add_id(csv_path: Path) -> None:
    """Añade columna unique_id (UUID-4 como texto)."""
    logging.info("Añadiendo unique_id…")
    df = pl.read_csv(csv_path)

    # genera n UUID y los convierte a string
    unique_ids = [str(uuid.uuid4()) for _ in range(df.height)]

    # añade la nueva columna
    df = df.with_columns(pl.Series("unique_id", unique_ids))

    df.write_csv(csv_path)


def update_datetime(csv_path: Path) -> None:
    """Normaliza accessed_at a ISO-8601 (YYYY-MM-DDTHH:MM:SSZ)."""
    logging.info("Normalizando accessed_at…")
    df = pl.read_csv(csv_path)

    df = df.with_columns(
        pl.col("accessed_at")
          .str.to_datetime("%Y-%m-%dT%H:%M:%S")   # parsea
          .dt.strftime("%Y-%m-%dT%H:%M:%SZ")       # vuelve a string + “Z”
    )

    df.write_csv(csv_path)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generador de lotes de datos.")
    p.add_argument("-r", "--records", type=int, default=100_372,
                   help="Registros a generar (default: 100 372).")
    p.add_argument("-o", "--out-dir", type=Path, default=Path("src_2/data_2"),
                   help="Directorio de salida.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    csv_path = write_to_csv(create_data(args.records), args.out_dir)
    add_id(csv_path)
    update_datetime(csv_path)
    logging.info("Archivo final listo en %s", csv_path)


if __name__ == "__main__":
    main()