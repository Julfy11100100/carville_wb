
FROM python:3.10.11-slim

RUN pip install --upgrade pip

COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

WORKDIR /app

COPY . .

ENV PYTHONPATH=/app

CMD ["python", "app/main.py"]
