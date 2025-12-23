import asyncpg
import os

DATABASE_URL = os.getenv("DATABASE_URL")

conn: asyncpg.Connection = None

async def init_db():
    global conn
    conn = await asyncpg.connect(DATABASE_URL)

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL PRIMARY KEY,
            topic TEXT NOT NULL,
            event_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            source TEXT NOT NULL,
            payload JSONB NOT NULL,
            UNIQUE(topic, event_id)
        )
    """)

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS stats (
            key TEXT PRIMARY KEY,
            value BIGINT NOT NULL
        )
    """)

    # init counters if empty
    for k in ["received", "unique", "duplicate"]:
        await conn.execute(
            "INSERT INTO stats (key, value) VALUES($1, 0) ON CONFLICT DO NOTHING",
            k
        )


async def insert_event(event):
    async with conn.transaction():
        await conn.execute(
            "UPDATE stats SET value = value + 1 WHERE key = 'received'"
        )

        result = await conn.execute("""
            INSERT INTO events(topic, event_id, timestamp, source, payload)
            VALUES($1,$2,$3,$4,$5)
            ON CONFLICT (topic, event_id) DO NOTHING
        """, event["topic"], event["event_id"], event["timestamp"], event["source"], event["payload"])

        if result == "INSERT 0 1":
            await conn.execute("UPDATE stats SET value = value + 1 WHERE key = 'unique'")
        else:
            await conn.execute("UPDATE stats SET value = value + 1 WHERE key = 'duplicate'")


async def get_all_events(topic=None):
    if topic:
        return await conn.fetch("SELECT * FROM events WHERE topic=$1", topic)
    return await conn.fetch("SELECT * FROM events")


async def get_stats():
    rows = await conn.fetch("SELECT key, value FROM stats")
    return {row["key"]: row["value"] for row in rows}
