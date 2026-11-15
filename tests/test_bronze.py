# tests/test_bronze.py — pruebas de la capa bronze, sin llamar a la API
from datetime import datetime, timezone

import polars as pl
import pytest

import bronze

UTC = timezone.utc


@pytest.fixture(autouse=True)
def raiz_temporal(tmp_path, monkeypatch):
    """Cada prueba escribe en una carpeta temporal, nunca en tus datos reales."""
    monkeypatch.setattr(bronze, "RAIZ", tmp_path)


def velas(instantes: list[datetime], precio: float = 100.0,
          cargado_en: datetime = datetime(2026, 1, 1, tzinfo=UTC)) -> pl.DataFrame:
    n = len(instantes)
    return pl.DataFrame({
        "ts": instantes,
        "low": [precio] * n, "high": [precio] * n,
        "open": [precio] * n, "close": [precio] * n,
        "volume": [1.0] * n,
        "cargado_en": [cargado_en] * n,
    })


def horas(*hs: int, dia: int = 1) -> list[datetime]:
    return [datetime(2026, 11, dia, h, tzinfo=UTC) for h in hs]


def test_repetir_la_carga_no_duplica():
    df = velas(horas(0, 1, 2))
    bronze.guardar(df, "1h")
    bronze.guardar(df, "1h")
    assert bronze.leer("1h").collect().height == 3


def test_la_carga_mas_reciente_gana():
    bronze.guardar(velas(horas(5), precio=100.0, cargado_en=datetime(2026, 1, 1, tzinfo=UTC)), "1h")
    bronze.guardar(velas(horas(5), precio=200.0, cargado_en=datetime(2026, 1, 2, tzinfo=UTC)), "1h")
    resultado = bronze.leer("1h").collect()
    assert resultado.height == 1
    assert resultado["close"][0] == 200.0


def test_una_carga_parcial_no_borra_lo_existente():
    bronze.guardar(velas(horas(0, 1, 2)), "1h")
    bronze.guardar(velas(horas(3, 4)), "1h")  # mismo día, horas nuevas
    assert bronze.leer("1h").collect().height == 5


def test_1h_particiona_por_dia():
    bronze.guardar(velas(horas(22, 23, dia=1) + horas(0, 1, dia=2)), "1h")
    archivos = list((tmp := bronze.RAIZ).rglob("velas.parquet"))
    assert len(archivos) == 2
    assert {a.parent.name for a in archivos} == {"fecha=2026-11-01", "fecha=2026-11-02"}


def test_1d_particiona_por_anio():
    dias = [datetime(2025, 12, 31, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2026, 1, 2, tzinfo=UTC)]
    bronze.guardar(velas(dias), "1d")
    archivos = list(bronze.RAIZ.rglob("velas.parquet"))
    assert {a.parent.name for a in archivos} == {"anio=2025", "anio=2026"}


def test_ultima_vela_es_la_marca_de_agua():
    assert bronze.ultima_vela("1h") is None
    bronze.guardar(velas(horas(3, 7, 5)), "1h")
    assert bronze.ultima_vela("1h") == datetime(2026, 11, 1, 7, tzinfo=UTC)