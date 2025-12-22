from fastapi import FastAPI
import threading
from generator import run

app = FastAPI()

@app.on_event("startup")
def startup():
    t = threading.Thread(target=run)
    t.start()

@app.get("/")
def health():
    return {"status": "running"}
