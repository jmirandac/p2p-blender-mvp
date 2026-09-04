FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN groupadd --system signaling \
    && useradd --system --gid signaling --no-create-home signaling

COPY requirements-signaling.txt ./
RUN pip install --no-cache-dir --requirement requirements-signaling.txt

COPY p2pchat ./p2pchat
COPY signaling_server.py ./

USER signaling

EXPOSE 9000

ENTRYPOINT ["python", "signaling_server.py"]
CMD ["--host", "0.0.0.0", "--port", "9000"]
