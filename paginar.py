# paginar.py — extracción paginada de velas desde Coinbase Exchange
import logging
import time
from datetime import datetime, timedelta, timezone

import polars as pl
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

URL = "https://api.exchange.coinbase.com/products/{par}/candles"
GRANULARIDADES = {60, 300, 900, 3600, 21600, 86400}
PRECIOS = ["low", "high", "open", "close", "volume"]
ESQUEMA = {"ts": pl.Datetime("us", "UTC"), **{c: pl.Float64 for c in PRECIOS}}


def _get_con_reintentos(session: requests.Session, url: str, params: dict,
                        intentos: int = 5, espera_base: float = 1.0) -> list:
    """GET con reintentos y espera exponencial (1s, 2s, 4s, 8s...).

    Reintenta solo errores transitorios: red, timeout, 429 (exceso de peticiones) y 5xx.
    Un 4xx distinto de 429 es un error nuestro (parámetros mal), así que falla de inmediato.
    """
    for intento in range(1, intentos + 1):
        try:
            resp = session.get(url, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            status = e.response.status_code if e.response is not None else None
            transitorio = status is None or status == 429 or status >= 500
            if not transitorio or intento == intentos:
                raise
            espera = espera_base * 2 ** (intento - 1)
            log.warning("Intento %s/%s falló (%s). Reintento en %.0fs", intento, intentos, e, espera)
            time.sleep(espera)
    raise RuntimeError("inalcanzable")


def _a_dataframe(filas: list) -> pl.DataFrame:
    if not filas:
        return pl.DataFrame(schema=ESQUEMA)
    return (
        pl.DataFrame(filas, schema=["ts", *PRECIOS], orient="row")
        .with_columns(
            pl.from_epoch("ts", time_unit="s").dt.replace_time_zone("UTC"),
            pl.col(PRECIOS).cast(pl.Float64),
        )
    )


def extraer_rango(inicio: datetime, fin: datetime, granularidad: int = 3600,
                  par: str = "BTC-USD", max_velas: int = 300, pausa: float = 0.35,
                  solo_cerradas: bool = True) -> pl.DataFrame:
    """Devuelve las velas de `par` en el intervalo [inicio, fin), paginando en ventanas."""
    if granularidad not in GRANULARIDADES:
        raise ValueError(f"Granularidad {granularidad} no válida. Usa una de {sorted(GRANULARIDADES)}")
    if inicio.tzinfo is None or fin.tzinfo is None:
        raise ValueError("inicio y fin deben tener zona horaria (usa tzinfo=timezone.utc)")
    if inicio >= fin:
        raise ValueError("inicio debe ser anterior a fin")

    url = URL.format(par=par)
    tamano_ventana = timedelta(seconds=max_velas * granularidad)
    partes = []

    with requests.Session() as session:
        inicio_ventana = inicio
        while inicio_ventana < fin:
            fin_ventana = min(inicio_ventana + tamano_ventana, fin)
            params = {
                "granularity": granularidad,
                "start": inicio_ventana.isoformat(),
                "end": fin_ventana.isoformat(),
            }
            df_ventana = _a_dataframe(_get_con_reintentos(session, url, params))
            log.info("Ventana %s → %s: %s velas", inicio_ventana, fin_ventana, df_ventana.height)
            partes.append(df_ventana)
            inicio_ventana = fin_ventana
            time.sleep(pausa)  # respeta el límite de peticiones de la API

    df = (
        pl.concat(partes)
        .unique(subset="ts")  # Coinbase incluye los bordes: hay solapes entre ventanas
        .filter((pl.col("ts") >= inicio) & (pl.col("ts") < fin))
        .sort("ts")
    )

    if solo_cerradas:
        ahora = datetime.now(timezone.utc)
        df = df.filter(pl.col("ts") + timedelta(seconds=granularidad) <= ahora)

    return df.with_columns(cargado_en=pl.lit(datetime.now(timezone.utc)))


if __name__ == "__main__":
    granularidad = 3600
    fin = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    inicio = fin - timedelta(days=30)

    df = extraer_rango(inicio, fin, granularidad)

    esperadas = int((fin - inicio).total_seconds() // granularidad)
    log.info("Velas: %s | únicas: %s | esperadas: %s | huecos: %s",
             df.height, df["ts"].n_unique(), esperadas, esperadas - df.height)
    print(df.head(3))
    print(df.tail(3))