FROM python:3.11-slim
WORKDIR /worker
COPY ./worker.py /worker/worker.py
RUN pip install --no-cache-dir redis requests
CMD ["python", "worker.py"]
