from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class RoomList(BaseModel):
    rooms: List[str]


class SensorTypeList(BaseModel):
    sensor_types: List[str]


class Reading(BaseModel):
    room: str
    sensor_type: str
    sensor_id: str
    ts_utc: datetime
    value_clean: Optional[float] = Field(None, description="Valeur nettoyée")
    unit: Optional[str] = None
    is_valid: bool
    quality_flag: str

    class Config:
        # Permet d'accepter des dicts venant de pandas avec des clés identiques
        orm_mode = True
