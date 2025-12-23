import asyncio
from db import insert_event

queue = asyncio.Queue()

async def enqueue(event):
    await queue.put(event)

async def worker():
    while True:
        event = await queue.get()
        await insert_event(event)
        queue.task_done()
