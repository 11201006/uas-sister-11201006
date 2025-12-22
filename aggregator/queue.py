import asyncio
import json
import asyncpg
from datetime import datetime
import os

DATABASE_URL = os.getenv("DATABASE_URL")

queue = asyncio.Queue()

async def enqueue(event):
    await queue.put(event)

async def worker():
    conn = await asyncpg.connect(DATABASE_URL)

    while True:
        event = await queue.get()
        try:
            async with conn.transaction():
                await conn.execute("""
                    INSERT INTO processed_events(topic, event_id, timestamp, source, payload)
                    VALUES($1, $2, $3, $4, $5)
                """, event["topic"], event["event_id"], event["timestamp"], event["source"], event["payload"])

                # update stats
                await conn.execute("""
                    UPDATE stats SET received = received + 1, unique_processed = unique_processed + 1 WHERE id = 1
                """)

        except asyncpg.UniqueViolationError:
            await conn.execute("""
                UPDATE stats SET received = received + 1, duplicate_dropped = duplicate_dropped + 1 WHERE id = 1
            """)

        except Exception as e:
            print("Error processing event:", e)

        await asyncio.sleep(0)  # let event loop breathe
