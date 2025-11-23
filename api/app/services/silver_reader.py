from datetime import datetime
from typing import List, Optional

import pandas as pd
import pyarrow.dataset as ds

from app.core.config import get_settings
from app.schemas.silver import Reading


def _normalize_s3_uri(path: str) -> str:
    """Normalise s3a:// en s3:// pour PyArrow."""
    if path.startswith("s3a://"):
        return "s3://" + path[len("s3a://") :]
    return path


def _build_dataset_for_date(event_date: str) -> ds.Dataset:
    """ Construit un Dataset PyArrow pour UNE date de partition"""
    settings = get_settings()
    root = _normalize_s3_uri(settings.SILVER_PATH).rstrip("/")

    path_for_date = f"{root}/event_date={event_date}/"
    dataset = ds.dataset(path_for_date, format="parquet")
    return dataset


def _parse_ts(ts_str: Optional[str]) -> Optional[datetime]:
    """Parse une chaîne ISO en datetime Python."""
    if ts_str is None:
        return None
    return pd.to_datetime(ts_str).to_pydatetime()


def list_rooms() -> List[str]:
    """Retourne la liste des rooms """
    dataset = _build_dataset_for_date("2013-08-23")
    table = dataset.to_table(columns=["room"])
    rooms = table["room"].unique().to_pylist()
    return sorted(str(r) for r in rooms if r is not None)


def list_sensor_types() -> List[str]:
    """Retourne la liste des sensor_type."""
    dataset = _build_dataset_for_date("2013-08-23")
    table = dataset.to_table(columns=["sensor_type"])
    sensor_types = table["sensor_type"].unique().to_pylist()
    return sorted(str(s) for s in sensor_types if s is not None)


def get_readings(
    event_date: str,
    room: Optional[str],
    sensor_type: Optional[str],
    from_ts_str: Optional[str],
    to_ts_str: Optional[str],
    only_valid: bool,
    limit: int,
) -> List[Reading]:
    """
    Lit les mesures Silver pour UNE date de partition (event_date).

    Les filtres possibles :
      - room
      - sensor_type
      - from_ts_str / to_ts_str (sur ts_utc)
      - only_valid
      - limit (nombre max de lignes)
    """
    settings = get_settings()
    max_limit = min(limit, settings.SILVER_MAX_LIMIT)
    dataset = _build_dataset_for_date(event_date)
    from_ts = _parse_ts(from_ts_str)
    to_ts = _parse_ts(to_ts_str)

    filter_expr = None

    if room is not None:
        expr = ds.field("room") == room
        filter_expr = expr if filter_expr is None else filter_expr & expr
    if sensor_type is not None:
        expr = ds.field("sensor_type") == sensor_type
        filter_expr = expr if filter_expr is None else filter_expr & expr

    if from_ts is not None:
        expr = ds.field("ts_utc") >= from_ts
        filter_expr = expr if filter_expr is None else filter_expr & expr
    if to_ts is not None:
        expr = ds.field("ts_utc") <= to_ts
        filter_expr = expr if filter_expr is None else filter_expr & expr
    if only_valid:
        expr = ds.field("is_valid") == True  # noqa: E712
        filter_expr = expr if filter_expr is None else filter_expr & expr

    wanted_cols = [
        "room",
        "sensor_type",
        "sensor_id",
        "ts_utc",
        "value_clean",
        "unit",
        "is_valid",
        "quality_flag",
    ]
    scanner = ds.Scanner.from_dataset(
        dataset,
        columns=wanted_cols,
        filter=filter_expr,
    )
    table = scanner.head(max_limit)
    df = table.to_pandas()
    if "ts_utc" in df.columns:
        df = df.sort_values("ts_utc")
    readings: List[Reading] = []
    for _, row in df.iterrows():
        readings.append(
            Reading(
                room=str(row.get("room")),
                sensor_type=str(row.get("sensor_type")),
                sensor_id=str(row.get("sensor_id")),
                ts_utc=row.get("ts_utc"),
                value_clean=row.get("value_clean"),
                unit=row.get("unit"),
                is_valid=bool(row.get("is_valid")),
                quality_flag=str(row.get("quality_flag")),
            )
        )
    return readings
