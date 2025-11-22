import os
import time
import redis
import subprocess
import requests

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
API_URL = os.getenv("API_URL", "http://api:8000")
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

STREAM_NAME = "jobs_stream"
GROUP = "worker_group"
CONSUMER = os.getenv("CONSUMER_NAME", "worker1")

# create consumer group if not exists
try:
    r.xgroup_create(STREAM_NAME, GROUP, id="$", mkstream=True)
except Exception as e:
    # group may already exist
    pass

print("Worker started, waiting for messages...")

while True:
    msgs = r.xreadgroup(GROUP, CONSUMER, {STREAM_NAME: ">"}, count=1, block=5000)
    if not msgs:
        continue
    for stream_name, messages in msgs:
        for msg_id, msg in messages:
            exec_id = msg.get("exec_id")
            job_id = msg.get("job_id")
            command = msg.get("command")
            print(f"Processing exec {exec_id}: {command}")

            # update execution to running
            try:
                requests.post(f"{API_URL}/internal/executions/{exec_id}/start")
            except Exception as e:
                print("Could not notify API about start:", e)

            # execute command (MVP: run local shell command)
            try:
                proc = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=300)
                logs = proc.stdout + "\n" + proc.stderr
                status = "success" if proc.returncode == 0 else "failed"
            except Exception as e:
                logs = str(e)
                status = "failed"

            # notify API of completion
            try:
                requests.post(f"{API_URL}/internal/executions/{exec_id}/finish", json={"status": status, "logs": logs})
            except Exception as e:
                print("Could not notify API about finish:", e)

            # ack message
            r.xack(STREAM_NAME, GROUP, msg_id)
