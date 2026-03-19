FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY q-backends-shared/ /q-backends-shared/
COPY q-cumber-backend/ .
RUN pip install --no-cache-dir -e /q-backends-shared && \
    pip install --no-cache-dir -e ".[dev]"

EXPOSE 3001

CMD ["qmap-qcumber-cloud"]
