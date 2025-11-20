from typing import List, Optional

from fastapi import APIRouter, Query

from app.schemas.silver import RoomList, SensorTypeList, Reading
from app.services.silver_reader import (
    list_rooms,
    list_sensor_types,
    get_readings,
)

router = APIRouter(tags=["silver"])


@router.get("/rooms", response_model=RoomList)
def get_rooms():
    """Liste des rooms disponibles dans la couche Silver."""
    rooms = list_rooms()
    return RoomList(rooms=rooms)


@router.get("/sensor-types", response_model=SensorTypeList)
def get_sensor_types():
    """Liste des types de capteurs dans la couche Silver."""
    sensor_types = list_sensor_types()
    return SensorTypeList(sensor_types=sensor_types)


@router.get("/readings", response_model=List[Reading])
def get_readings_endpoint(
    event_date: str = Query(
        ...,
        description="Date de partition au format YYYY-MM-DD (ex: 2013-08-23)",
    ),
    room: Optional[str] = Query(None, description="Filtrer par room"),
    sensor_type: Optional[str] = Query(None, description="Filtrer par type de capteur"),
    from_ts: Optional[str] = Query(
        None,
        alias="from",
        description="Date/heure minimale (ISO, ex: 2013-08-23T00:00:00)",
    ),
    to_ts: Optional[str] = Query(
        None,
        alias="to",
        description="Date/heure maximale (ISO)",
    ),
    only_valid: bool = Query(
        True,
        description="True = ne renvoyer que les mesures valides",
    ),
    limit: int = Query(
        100,
        ge=1,
        le=1000,
        description="Nombre max de lignes retournées",
    ),
):
    """
    Lit les mesures Silver pour UNE date de partition.
    """
    readings = get_readings(
        event_date=event_date,
        room=room,
        sensor_type=sensor_type,
        from_ts_str=from_ts,
        to_ts_str=to_ts,
        only_valid=only_valid,
        limit=limit,
    )
    return readings

