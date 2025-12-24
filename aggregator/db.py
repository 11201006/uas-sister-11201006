import asyncpg
import asyncio
import os
import json

DATABASE_URL = os.getenv("DATABASE_URL")

async def connect():
    return await asyncpg.connect(DATABASE_URL)


async def init_db():
    # Retry connect until DB ready
    for i in range(10):
        try:
            conn = await connect()

            # Main event table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    topic TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    timestamp TEXT,
                    source TEXT,
                    payload JSONB,
                    PRIMARY KEY (topic, event_id)
                );
            """)

            # Stats table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS stats (
                    key TEXT PRIMARY KEY,
                    value BIGINT
                );
            """)

            # Initialize counters
            await conn.execute("""
                INSERT INTO stats (key, value) VALUES
                    ('received', 0),
                    ('unique_processed', 0),
                    ('duplicate_dropped', 0)
                ON CONFLICT (key) DO NOTHING;
            """)

            await conn.close()
            print("DB is ready")
            return
        except Exception:
            print(f"DB not ready, retrying...")
            await asyncio.sleep(1)

    raise RuntimeError("Postgres failed to start after retries")


async def insert_event(event):
    """
    Atomic dedup using ON CONFLICT DO NOTHING
    Returns:
        True if inserted (unique)
        False if duplicate
    """

    conn = await connect()

    try:
        result = await conn.execute("""
            INSERT INTO events (topic, event_id, timestamp, source, payload)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT DO NOTHING;
        """, event["topic"], event["event_id"], event["timestamp"],
             event["source"], json.dumps(event["payload"]))

        # "INSERT 0 1" → inserted
        inserted = result.endswith("1")

        if inserted:
            await conn.execute("UPDATE stats SET value = value + 1 WHERE key = 'unique_processed';")
        else:
            await conn.execute("UPDATE stats SET value = value + 1 WHERE key = 'duplicate_dropped';")

        return inserted

    finally:
        await conn.close()


async def bump_received(n):
    conn = await connect()
    await conn.execute("UPDATE stats SET value = value + $1 WHERE key = 'received';", n)
    await conn.close()


async def get_events_by_topic(topic):
    conn = await connect()
    rows = await conn.fetch("SELECT * FROM events WHERE topic = $1;", topic)
    await conn.close()

    return [
        {
            "topic": r["topic"],
            "event_id": r["event_id"],
            "timestamp": r["timestamp"],
            "source": r["source"],
            "payload": r["payload"]
        }
        for r in rows
    ]


async def get_stats():
    conn = await connect()
    rows = await conn.fetch("SELECT key, value FROM stats;")
    await conn.close()

    return {r["key"]: r["value"] for r in rows}
