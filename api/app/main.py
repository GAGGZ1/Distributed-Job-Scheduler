from fastapi import FastAPI, Depends, HTTPException, Body
import db, models, schemas, crud
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
import uvicorn
import os
import redis
import json

app = FastAPI(title="Distributed Scheduler API")

# init db
models.Base.metadata.create_all(bind=db.engine)

def get_db():
    database = db.SessionLocal()
    try:
        yield database
    finally:
        database.close()

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

STREAM_NAME = "jobs_stream"

# --- PUBLIC ENDPOINTS ---

@app.post("/api/v1/jobs", response_model=schemas.JobOut)
def create_job(job_in: schemas.JobCreate, database: Session = Depends(get_db)):
    job = crud.create_job(database, job_in)
    return job

@app.get("/api/v1/jobs")
def list_jobs(database: Session = Depends(get_db)):
    return crud.list_jobs(database)

@app.post("/api/v1/jobs/{job_id}/run")
def trigger_job_run(job_id: str, database: Session = Depends(get_db)):
    job = crud.get_job(database, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # create execution record
    exec = crud.create_execution(database, job.id)
    
    # push to redis stream with exec id and command
    r.xadd(STREAM_NAME, {"exec_id": str(exec.id), "job_id": str(job.id), "command": job.command})
    return {"status": "queued", "execution_id": str(exec.id)}

# --- INTERNAL WORKER ENDPOINTS ---

@app.post("/internal/executions/{exec_id}/start")
def start_execution(exec_id: str, database: Session = Depends(get_db)):
    from crud import update_execution
    update_execution(database, exec_id, status="running", started_at=func.now())
    return {"ok": True}

@app.post("/internal/executions/{exec_id}/finish")
def finish_execution(exec_id: str, payload: dict = Body(...), database: Session = Depends(get_db)):
    status = payload.get("status")
    logs = payload.get("logs")
    from crud import update_execution
    update_execution(database, exec_id, status=status, finished_at=func.now(), logs=logs)
    return {"ok": True}