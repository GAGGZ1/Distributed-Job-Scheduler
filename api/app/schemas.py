from pydantic import BaseModel
from typing import Optional
from uuid import UUID

class JobCreate(BaseModel):
    name: str
    command: str
    schedule: Optional[str] = None
    retries: Optional[int] = 0

class JobOut(BaseModel):
    id: UUID
    name: str
    command: str
    schedule: Optional[str]
    retries: int
