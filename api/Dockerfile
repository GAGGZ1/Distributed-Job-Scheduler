FROM python:3.11-slim
WORKDIR /app
COPY ./app /app
RUN pip install --no-cache-dir fastapi uvicorn sqlalchemy psycopg2-binary redis pydantic croniter
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
