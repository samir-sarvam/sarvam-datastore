from pydantic import BaseModel


class GeoPoint(BaseModel):
    latitude: float = 0.0
    longitude: float = 0.0
