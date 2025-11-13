FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Dossier de travail dans le conteneur   
WORKDIR /app

COPY scripts/replay/requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/replay/ ./replay/

CMD ["python", "-u", "replay/producer.py"]