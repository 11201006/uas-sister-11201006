from fastapi import FastAPI, HTTPException
import asyncio
from queue import enqueue, worker
from models import Event
from db import init_db
import asyncpg
import os

app = FastAPI()
DATABASE_URL = os.getenv("DATABASE_URL")

@app.on_event("startup")
async def startup():
    await init_db()

    # start multiple workers for concurrency
    for _ in range(4):
        asyncio.create_task(worker())

@app.post("/publish")
async def publish(event: Event):
    await enqueue(event.dict())
    return {"status": "queued"}

@app.get("/events")
async def events(topic: str = None):
    conn = await asyncpg.connect(DATABASE_URL)
    if topic:
        rows = await conn.fetch("SELECT * FROM processed_events WHERE topic = $1", topic)
    else:
        rows = await conn.fetch("SELECT * FROM processed_events")
    await conn.close()
    return rows

@app.get("/stats")
async def stats():
    conn = await asyncpg.connect(DATABASE_URL)
    row = await conn.fetchrow("SELECT * FROM stats WHERE id = 1")
    await conn.close()
    return dict(row)
