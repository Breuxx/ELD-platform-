import os
import datetime
from typing import List

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import Column, Integer, String, DateTime, JSON, create_engine, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@db:5432/eld_db")
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
    meta = Column(JSON, nullable=True)

Base.metadata.create_all(bind=engine)

class LogEntrySchema(BaseModel):
    id: int
    driver_id: str
    timestamp: datetime.datetime
    event_type: str
    meta: dict | None

    class Config:
        orm_mode = True

app = FastAPI(title="ELD Platform API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/api/logs", response_model=List[LogEntrySchema])
def get_logs(
    start: datetime.datetime = Query(...),
    end:   datetime.datetime = Query(...),
    db:    Session = Depends(get_db)
):
    if start > end:
        raise HTTPException(400, "Start must be before end")
    return db.query(LogEntry)\
             .filter(and_(LogEntry.timestamp >= start, LogEntry.timestamp <= end))\
             .order_by(LogEntry.timestamp.desc())\
             .all()
