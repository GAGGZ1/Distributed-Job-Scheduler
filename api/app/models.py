from sqlalchemy import Column, String, Integer, DateTime, Text, Enum, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
import uuid
from sqlalchemy.sql import func
from db import Base
import enum

class JobStatus(enum.Enum):
    scheduled = "scheduled"
    disabled = "disabled"

class ExecutionStatus(enum.Enum):
    queued = "queued"
    running = "running"
    success = "success"
    failed = "failed"

class Job(Base):
    __tablename__ = "jobs"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    command = Column(Text, nullable=False)   # for MVP, store shell command
    schedule = Column(String, nullable=True) # cron string (later)
    retries = Column(Integer, default=0)
    status = Column(Enum(JobStatus), default=JobStatus.scheduled)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    next_run_at = Column(DateTime(timezone=True), nullable=True)
    

class Execution(Base):
    __tablename__ = "executions"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id"), nullable=False)
    status = Column(Enum(ExecutionStatus), default=ExecutionStatus.queued)
    attempts = Column(Integer, default=0)
    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    logs = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
