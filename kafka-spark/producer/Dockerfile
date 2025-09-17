FROM python:3.11-slim
WORKDIR /app
COPY producer.py /app/
RUN pip install --no-cache-dir kafka-python
ENV PYTHONUNBUFFERED=1
CMD ["python", "-u", "producer.py"]
