FROM python:3.10.11-slim

RUN pip install --upgrade pip

COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

WORKDIR /project

# Копируем всё содержимое проекта в /project
COPY . .

# Устанавливаем PYTHONPATH чтобы импорты работали корректно
ENV PYTHONPATH=/project

# Запускаем как модуль
CMD ["python", "-m", "app.main"]
