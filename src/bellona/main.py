from fastapi import FastAPI

from bellona.api.v1 import router as v1_router

app = FastAPI(title="Bellona", version="0.1.0")
app.include_router(v1_router)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
