FROM python:3.10-slim

WORKDIR /app

COPY sf_exporter.py /app/sf_exporter.py

RUN pip install snowflake-connector-python prometheus_client

EXPOSE 8000

CMD ["python", "sf_exporter.py"]
