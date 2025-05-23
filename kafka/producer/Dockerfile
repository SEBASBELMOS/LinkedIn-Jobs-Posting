FROM python:3.11-slim
WORKDIR /app
COPY producer.py requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "producer.py"]