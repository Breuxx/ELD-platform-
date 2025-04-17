import os
import sys
import datetime
import asyncio
import json

from fastapi import FastAPI, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict
from sqlalchemy import Column, Integer, String, DateTime, JSON as SAJSON, create_engine, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import aioredis

# 1. Проверяем обязательные переменные окружения
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("❌ ERROR: DATABASE_URL is not set", file=sys.stderr)
    sys.exit(1)

REDIS_URL = os.getenv("REDIS_URL")
if not REDIS_URL:
    print("❌ ERROR: REDIS_URL is not set", file=sys.stderr)
    sys.exit(1)

# 2. Настройка базы данных (PostgreSQL)
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()

class Driver(Base):
    __tablename__ = "drivers"
    id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False)

class LogEntry(Base):
    __tablename__ = "log_entries"
    id = Column(Integer, primary_key=True, index=True)
    driver_id = Column(String, index=True)
    timestamp = Column(DateTime, index=True)
    event_type = Column(String, nullable=False)
    meta = Column(SAJSON, nullable=True)

# Создаём таблицы (на проде лучше через Alembic)
Base.metadata.create_all(bind=engine)

# 3. Pydantic‑схема для отдачи логов
class LogEntrySchema(BaseModel):
    id: int
    driver_id: str
    timestamp: datetime.datetime
    event_type: str
    meta: dict | None

    model_config = ConfigDict(from_attributes=True)

# 4. Инициализация FastAPI + CORS
app = FastAPI(title="ELD Platform API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 5. Настройка Redis Pub/Sub
redis = aioredis.from_url(REDIS_URL, decode_responses=True)
pubsub = redis.pubsub()

# Хранилище подключённых WebSocket‑соеднинений
connections: set[WebSocket] = set()

# Зависимость: сессия БД
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# 6. При старте подписываемся на канал и запускаем слушатель
@app.on_event("startup")
async def startup_event():
    await pubsub.subscribe("eld_events")
    asyncio.create_task(_redis_listener())

async def _redis_listener():
    async for msg in pubsub.listen():
        if msg and msg["type"] == "message":
            data = msg["data"]  # строка JSON
            to_remove = []
            for ws in connections:
                try:
                    await ws.send_text(data)
                except WebSocketDisconnect:
                    to_remove.append(ws)
            for ws in to_remove:
                connections.remove(ws)

# 7. REST‑эндпоинты

@app.get(
    "/api/logs",
    response_model=list[LogEntrySchema],
    summary="Get log entries for a given time range"
)
def get_logs(
    start: datetime.datetime = Query(..., description="Start ISO timestamp"),
    end:   datetime.datetime = Query(..., description="End ISO timestamp"),
    db:    Session = Depends(get_db)
):
    if start > end:
        raise HTTPException(400, "Start must be before end")
    entries = (
        db.query(LogEntry)
          .filter(and_(LogEntry.timestamp >= start, LogEntry.timestamp <= end))
          .order_by(LogEntry.timestamp.desc())
          .all()
    )
    return entries

@app.post("/api/eld/logs", status_code=204, summary="Receive a log entry from driver")
async def receive_log(
    entry: dict = Body(..., description="Event JSON from ELD device"),
    db:    Session = Depends(get_db)
):
    # Сохраняем в базу
    try:
        ts = entry.get("timestamp")
        # Если приходит в формате ISO с 'Z'
        if ts.endswith("Z"):
            ts = ts.replace("Z", "+00:00")
        log = LogEntry(
            driver_id=entry.get("driverId"),
            timestamp=datetime.datetime.fromisoformat(ts),
            event_type=entry.get("status"),
            meta=entry
        )
        db.add(log)
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(400, f"DB error: {e}")
    # Публикуем в Redis для WS-клиентов
    await redis.publish("eld_events", json.dumps(entry))
    return

# 8. WebSocket‑эндпоинт для реального времени
@app.websocket("/ws/eld")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    connections.add(ws)
    try:
        while True:
            # Просто держим соединение живым
            await asyncio.sleep(30)
    except WebSocketDisconnect:
        connections.remove(ws)