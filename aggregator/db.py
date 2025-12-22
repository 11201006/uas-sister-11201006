import asyncpg
import os
DATABASE_URL = os.getenv("DATABASE_URL")

async def init_db():
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_events (
            topic TEXT NOT NULL,
            event_id TEXT NOT NULL,
            timestamp TEXT,
            source TEXT,
            payload JSONB,
            PRIMARY KEY (topic, event_id)
        );
    """)

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS stats (
            id INT PRIMARY KEY,
            received BIGINT,
            unique_processed BIGINT,
            duplicate_dropped BIGINT
        );
        INSERT INTO stats(id, received, unique_processed, duplicate_dropped)
        VALUES(1,0,0,0)
        ON CONFLICT (id) DO NOTHING;
    """)
    await conn.close()
