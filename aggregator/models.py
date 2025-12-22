from pydantic import BaseModel
from typing import Dict
from datetime import datetime

class Event(BaseModel):
    topic: str
    event_id: str
    timestamp: str
    source: str
    payload: Dict
