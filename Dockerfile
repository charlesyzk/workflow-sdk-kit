FROM python:3.11-slim

WORKDIR /app
COPY sdk /app/sdk
COPY starter /app/starter
RUN python -m pip install --no-cache-dir /app/sdk /app/starter
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "uvicorn", "starter_app.main:app", "--host", "0.0.0.0", "--port", "8090"]

