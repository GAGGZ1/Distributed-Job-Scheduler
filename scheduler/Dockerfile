FROM python:3.11-slim
WORKDIR /app
COPY scheduler.py .
RUN pip install --no-cache-dir sqlalchemy psycopg2-binary redis croniter
CMD ["python", "scheduler.py"]