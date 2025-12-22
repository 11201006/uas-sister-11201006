import asyncio
import json
import redis
import asyncpg
import os

BROKER_URL = os.getenv("BROKER_URL")
DATABASE_URL = os.getenv("DATABASE_URL")

redis_client = redis.Redis.from_url(BROKER_URL)

async def process_event(event_json):
    conn = await asyncpg.connect(DATABASE_URL)
    event = json.loads(event_json)

    async with conn.transaction():
        try:
            await conn.execute("""
                INSERT INTO processed_events(topic, event_id, timestamp, source, payload)
                VALUES($1, $2, $3, $4, $5)
            """, event["topic"], event["event_id"], event["timestamp"], event["source"], event["payload"])
            
            await conn.execute("UPDATE stats SET unique_processed = unique_processed + 1, received = received + 1 WHERE id = 1")

        except asyncpg.UniqueViolationError:
            await conn.execute("UPDATE stats SET duplicate_dropped = duplicate_dropped + 1, received = received + 1 WHERE id = 1")
        
    await conn.close()

async def worker():
    while True:
        msg = redis_client.lpop("events")
        if msg:
            await process_event(msg)
        await asyncio.sleep(0.01)
