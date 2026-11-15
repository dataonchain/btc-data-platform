# extraer.py — primera extracción de velas horarias de BTC-USD
import logging
from datetime import datetime, timezone

import polars as pl
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
URL = "https://api.exchange.coinbase.com/products/BTC-USD/candles"


def extraer_velas(granularidad: int = 3600) -> pl.DataFrame:
    resp = requests.get(URL, params={"granularity": granularidad}, timeout=10)
    resp.raise_for_status()
    filas = resp.json()  # [time, low, high, open, close, volume]
    df = pl.DataFrame(
        filas,
        schema=["ts", "low", "high", "open", "close", "volume"],
        orient="row",
    ).with_columns(
        pl.from_epoch("ts", time_unit="s").dt.replace_time_zone("UTC"),
        pl.lit(datetime.now(timezone.utc)).alias("cargado_en"),
    ).sort("ts")
    logging.info("Velas recibidas: %s", df.height)
    return df


if __name__ == "__main__":
    extraer_velas().write_parquet("velas_1h.parquet")