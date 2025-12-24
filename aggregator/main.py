from fastapi import FastAPI, Request
from typing import List, Union
from models import EventModel
from db import init_db, insert_event, get_events_by_topic, bump_received, get_stats

app = FastAPI(title="Distributed Log Aggregator")

@app.on_event("startup")
async def startup():
    await init_db()

@app.post("/publish")
async def publish(events: Union[EventModel, List[EventModel]]):

    # Normalize to list
    if isinstance(events, EventModel):
        events = [events]

    await bump_received(len(events))

    results = []
    for ev in events:
        inserted = await insert_event(ev.dict())
        results.append({"event_id": ev.event_id, "stored": inserted})

    return {"processed": len(events), "results": results}


@app.get("/events")
async def list_events(topic: str):
    return await get_events_by_topic(topic)


@app.get("/stats")
async def stats():
    return await get_stats()
