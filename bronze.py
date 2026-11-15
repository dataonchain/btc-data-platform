# bronze.py — capa bronze: almacenamiento particionado e idempotente de velas
import argparse
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from paginar import extraer_rango

log = logging.getLogger(__name__)

RAIZ = Path("data/bronze")
FUENTE = "coinbase"
FRECUENCIAS = {"1m": 60, "1h": 3600, "1d": 86400}


def _ahora_redondeado(granularidad: int) -> datetime:
    segundos = int(datetime.now(timezone.utc).timestamp()) // granularidad * granularidad
    return datetime.fromtimestamp(segundos, tz=timezone.utc)


def _desde_defecto(frecuencia: str) -> datetime:
    if frecuencia == "1d":
        return datetime(2015, 7, 20, tzinfo=timezone.utc)  # inicio de BTC-USD en Coinbase
    if frecuencia == "1h":
        return datetime(2024, 1, 1, tzinfo=timezone.utc)
    return _ahora_redondeado(60) - timedelta(days=30)      # 1m: solo 30 días


def _base(frecuencia: str, par: str) -> Path:
    return RAIZ / f"fuente={FUENTE}" / f"par={par}" / f"frecuencia={frecuencia}"


def _clave_particion(frecuencia: str) -> tuple[str, str]:
    # 1d tiene una fila por día: particionar por día crearía miles de archivos diminutos
    return ("anio", "%Y") if frecuencia == "1d" else ("fecha", "%Y-%m-%d")


def guardar(df: pl.DataFrame, frecuencia: str, par: str = "BTC-USD") -> int:
    """Upsert por partición: une lo nuevo con lo existente, deduplica por ts
    (gana la carga más reciente) y reescribe la partición de forma atómica."""
    if df.is_empty():
        log.info("Nada que guardar")
        return 0

    clave, formato = _clave_particion(frecuencia)
    df = df.with_columns(pl.col("ts").dt.strftime(formato).alias("_particion"))

    for parte in df.partition_by("_particion"):
        valor = parte["_particion"][0]
        parte = parte.drop("_particion")
        carpeta = _base(frecuencia, par) / f"{clave}={valor}"
        carpeta.mkdir(parents=True, exist_ok=True)
        archivo = carpeta / "velas.parquet"

        if archivo.exists():
            parte = pl.concat([pl.read_parquet(archivo), parte], how="vertical_relaxed")

        parte = parte.sort("cargado_en").unique(subset="ts", keep="last").sort("ts")

        temporal = archivo.with_suffix(".tmp")
        parte.write_parquet(temporal)
        os.replace(temporal, archivo)  # atómico: nunca queda un archivo a medio escribir

    log.info("Guardadas %s velas en %s particiones de %s", df.height,
             df["_particion"].n_unique(), frecuencia)
    return df.height


def leer(frecuencia: str, par: str = "BTC-USD") -> pl.LazyFrame | None:
    base = _base(frecuencia, par)
    if not any(base.rglob("velas.parquet")):
        return None
    return pl.scan_parquet(str(base / "**" / "velas.parquet"), hive_partitioning=False)


def ultima_vela(frecuencia: str, par: str = "BTC-USD") -> datetime | None:
    lf = leer(frecuencia, par)
    return None if lf is None else lf.select(pl.col("ts").max()).collect().item()


def cargar_incremental(frecuencia: str, desde: datetime | None = None, par: str = "BTC-USD") -> None:
    """Trae solo lo nuevo: desde la última vela guardada hasta la última vela cerrada."""
    granularidad = FRECUENCIAS[frecuencia]
    ultima = ultima_vela(frecuencia, par)
    inicio = ultima + timedelta(seconds=granularidad) if ultima else (desde or _desde_defecto(frecuencia))
    fin = _ahora_redondeado(granularidad)

    if inicio >= fin:
        log.info("%s ya está al día (última vela: %s)", frecuencia, ultima)
        return

    log.info("Carga incremental %s: %s → %s", frecuencia, inicio, fin)
    guardar(extraer_rango(inicio, fin, granularidad, par), frecuencia, par)


def backfill(frecuencia: str, desde: datetime, hasta: datetime, par: str = "BTC-USD") -> None:
    """Rehace un rango completo. Gracias al upsert, repetirlo no duplica datos."""
    granularidad = FRECUENCIAS[frecuencia]
    hasta = min(hasta, _ahora_redondeado(granularidad))
    log.info("Backfill %s: %s → %s", frecuencia, desde, hasta)
    guardar(extraer_rango(desde, hasta, granularidad, par), frecuencia, par)


def resumen(par: str = "BTC-USD") -> None:
    for frecuencia, granularidad in FRECUENCIAS.items():
        lf = leer(frecuencia, par)
        if lf is None:
            print(f"{frecuencia}: sin datos")
            continue
        r = lf.select(
            filas=pl.len(),
            unicas=pl.col("ts").n_unique(),
            primera=pl.col("ts").min(),
            ultima=pl.col("ts").max(),
        ).collect().row(0, named=True)
        esperadas = int((r["ultima"] - r["primera"]).total_seconds() // granularidad) + 1
        archivos = len(list(_base(frecuencia, par).rglob("velas.parquet")))
        print(f"{frecuencia}: {r['filas']} filas | {r['unicas']} únicas | "
              f"{r['primera']} → {r['ultima']} | huecos: {esperadas - r['filas']} | "
              f"archivos: {archivos}")


def _fecha(texto: str) -> datetime:
    return datetime.fromisoformat(texto).replace(tzinfo=timezone.utc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Capa bronze de velas BTC")
    sub = parser.add_subparsers(dest="comando", required=True)

    p_inc = sub.add_parser("incremental", help="Trae solo las velas nuevas")
    p_inc.add_argument("--frecuencia", choices=FRECUENCIAS, required=True)
    p_inc.add_argument("--desde", type=_fecha, help="Solo se usa si aún no hay datos")

    p_bf = sub.add_parser("backfill", help="Rehace un rango de fechas")
    p_bf.add_argument("--frecuencia", choices=FRECUENCIAS, required=True)
    p_bf.add_argument("--desde", type=_fecha, required=True)
    p_bf.add_argument("--hasta", type=_fecha, default=datetime.now(timezone.utc))

    sub.add_parser("resumen", help="Muestra el estado de la capa bronze")

    args = parser.parse_args()
    if args.comando == "incremental":
        cargar_incremental(args.frecuencia, args.desde)
    elif args.comando == "backfill":
        backfill(args.frecuencia, args.desde, args.hasta)
    else:
        resumen()