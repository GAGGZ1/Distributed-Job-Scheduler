from sqlalchemy.orm import Session
import models, schemas

def create_job(db: Session, job_in: schemas.JobCreate):
    job = models.Job(name=job_in.name, command=job_in.command, schedule=job_in.schedule, retries=job_in.retries)
    db.add(job)
    db.commit()
    db.refresh(job)
    return job

def get_job(db: Session, job_id):
    return db.query(models.Job).filter(models.Job.id == job_id).first()

def list_jobs(db: Session, limit: int = 50):
    return db.query(models.Job).order_by(models.Job.created_at.desc()).limit(limit).all()

def create_execution(db: Session, job_id):
    exec = models.Execution(job_id=job_id)
    db.add(exec)
    db.commit()
    db.refresh(exec)
    return exec

def update_execution(db: Session, exec_id, **kwargs):
    exec = db.query(models.Execution).filter(models.Execution.id == exec_id).first()
    for k, v in kwargs.items():
        setattr(exec, k, v)
    db.commit()
    db.refresh(exec)
    return exec
