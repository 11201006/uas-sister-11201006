from fastapi import FastAPI, HTTPException
from db import init_db, get_all_events, get_stats
from models import Event
from queue_worker import enqueue, worker
import asyncio

app = FastAPI()


@app.on_event("startup")
async def startup():
    await init_db()
    # launch 3 workers
    for _ in range(3):
        asyncio.create_task(worker())


@app.post("/publish")
async def publish(event: Event):
    await enqueue(event.dict())
    return {"status": "queued"}


@app.get("/events")
async def list_events(topic: str = None):
    events = await get_all_events(topic)
    return [dict(e) for e in events]


@app.get("/stats")
async def stats():
    return await get_stats()
