# BTC Data Platform

Plataforma de datos de precios de Bitcoin (BTC-USD) en tres frecuencias: 1 minuto, 1 hora y 1 día.
Proyecto de aprendizaje práctico de ingeniería de datos: ingesta desde APIs, data lake,
data warehouse en la nube, orquestación, streaming, calidad de datos y CI/CD.

## Estado actual

**Fase 0 completada:** extracción de velas horarias desde la API pública de Coinbase Exchange
y almacenamiento en formato Parquet.

## Requisitos

- Linux o Windows con WSL2 (Ubuntu 22.04)
- [uv](https://docs.astral.sh/uv/) y Python 3.12
- Git

## Cómo ejecutarlo

```bash
git clone git@github.com:dataonchain/btc-data-platform.git
cd btc-data-platform
uv sync
uv run extraer.py
```

El script descarga las velas horarias más recientes de BTC-USD y las guarda en `velas_1h.parquet`.

Para ver los datos:

```bash
uv run python -c "import polars as pl; print(pl.read_parquet('velas_1h.parquet').tail())"
```

## Datos

Cada fila es una vela (OHLCV) de una hora:

| Columna | Descripción |
| --- | --- |
| `ts` | Inicio del intervalo, en UTC |
| `open` | Precio de la primera operación del intervalo (USD) |
| `high` | Precio máximo del intervalo (USD) |
| `low` | Precio mínimo del intervalo (USD) |
| `close` | Precio de la última operación del intervalo (USD) |
| `volume` | Volumen negociado, en BTC |
| `cargado_en` | Momento de la extracción, en UTC (metadato de linaje) |

Fuente: [Coinbase Exchange API – Get product candles](https://docs.cdp.coinbase.com/exchange/reference/exchangerestapi_getproductcandles)

## Hallazgos y decisiones

- **La documentación no siempre coincide con la API.** La documentación de Coinbase indica un máximo
  de 300 velas por petición, pero la API devolvió 350. El límite por petición será un parámetro
  configurable, no un valor fijo en el código.
- **La última vela está incompleta.** La vela de la hora en curso sigue cambiando hasta que esa hora
  termina. La ingesta deberá descartarla o sobrescribirla en cargas posteriores sin duplicar datos.
- **Todo en UTC.** Todas las marcas de tiempo se guardan en UTC para evitar errores entre zonas horarias.
- **Los datos no se versionan.** Los archivos `.parquet` están excluidos en `.gitignore`: el repositorio
  guarda el código que genera los datos, no los datos.
- **Coinbase no publica intervalos sin operaciones**, así que la serie puede tener huecos que deben
  detectarse y tratarse.

## Hoja de ruta

- [x] Fase 0 – Entorno y primera extracción
- [ ] Fase 1 – Ingesta batch robusta: paginación, reintentos, idempotencia, capa bronze
- [ ] Fase 2 – Modelado SQL con DuckDB y dbt (capas silver y gold)
- [ ] Fase 3 – Nube en GCP (BigQuery y Cloud Storage) con Terraform
- [ ] Fase 4 – Orquestación con Airflow
- [ ] Fase 5 – Streaming en tiempo real (Redpanda y Apache Beam)
- [ ] Fase 6 – Calidad de datos, varias fuentes y gobernanza
- [ ] Fase 7 – CI/CD y observabilidad
- [ ] Fase 8 – Dashboard y modelo de volatilidad
- [ ] Fase 9 – Documentación final y portafolio

## Stack

Python · uv · Polars · Parquet · Docker · WSL2 · Git/GitHub