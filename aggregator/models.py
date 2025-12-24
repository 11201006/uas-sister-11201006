from pydantic import BaseModel
from typing import Dict, Any

class EventModel(BaseModel):
    topic: str
    event_id: str
    timestamp: str
    source: str
    payload: Dict[str, Any]
