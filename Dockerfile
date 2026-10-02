FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common.py namenode.py datanode.py dfs_client.py client.py ./
COPY templates ./templates

# run as an unprivileged user; state lives in /data (a volume)
RUN useradd --create-home --uid 10001 hdfs && mkdir /data && chown hdfs /data
USER hdfs
VOLUME ["/data"]

# One image, three roles. docker-compose picks the role with `command:`.
CMD ["python", "namenode.py"]
