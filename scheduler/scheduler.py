import time
import os
import redis
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError
from croniter import croniter
from datetime import datetime, timezone

# Configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/postgres")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
STREAM_NAME = "jobs_stream"

# Setup Connections
engine = create_engine(DATABASE_URL)
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

def wait_for_tables():
    """Blocks until the 'jobs' table exists in the database."""
    print("Scheduler: Waiting for database tables to be created...")
    while True:
        try:
            with engine.connect() as connection:
                # Trying a simple query to check if the table exists
                connection.execute(text("SELECT 1 FROM jobs LIMIT 1"))
                print("Scheduler: Tables found! Starting main loop.")
                return
        except (OperationalError, ProgrammingError):
            # Table doesn't exist yet, or DB is warming up
            print("Scheduler: Database not ready yet. Retrying in 2s...")
            time.sleep(2)

# === BLOCK HERE UNTIL TABLES EXIST ===
wait_for_tables()

print("Scheduler Service Started...")

while True:
    try:
        with engine.connect() as connection:
            # 1. Find jobs that are due
            now = datetime.now(timezone.utc)
            query = text("""
                SELECT id, name, command, schedule 
                FROM jobs 
                WHERE status = 'scheduled' 
                AND next_run_at <= :now
            """)
            result = connection.execute(query, {"now": now})
            due_jobs = result.fetchall()

            for job in due_jobs:
                job_id = str(job.id)
                print(f"Triggering scheduled job: {job.name} ({job_id})")

                # 2. Create Execution Record
                insert_exec = text("""
                    INSERT INTO executions (id, job_id, status, attempts, created_at)
                    VALUES (gen_random_uuid(), :job_id, 'queued', 0, :now)
                    RETURNING id
                """)
                exec_result = connection.execute(insert_exec, {"job_id": job_id, "now": now})
                exec_id = str(exec_result.fetchone()[0])
                connection.commit()

                # 3. Push to Redis
                r.xadd(STREAM_NAME, {
                    "exec_id": exec_id, 
                    "job_id": job_id, 
                    "command": job.command
                })

                # 4. Update Next Run Time
                if job.schedule:
                    iter = croniter(job.schedule, now)
                    next_run = iter.get_next(datetime)
                    
                    update_query = text("UPDATE jobs SET next_run_at = :next_run WHERE id = :job_id")
                    connection.execute(update_query, {"next_run": next_run, "job_id": job_id})
                    connection.commit()
                    print(f"  -> Rescheduled for {next_run}")
                else:
                    connection.execute(text("UPDATE jobs SET next_run_at = NULL WHERE id = :job_id"), {"job_id": job_id})
                    connection.commit()

    except Exception as e:
        print(f"Scheduler error: {e}")
    
    time.sleep(10)