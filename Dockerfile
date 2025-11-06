FROM python:3.10.11-slim

RUN sed -i 's|http://deb.debian.org|https://deb.debian.org|g' /etc/apt/sources.list \
    && apt-get update \
    && apt-get install -y curl gnupg unixodbc unixodbc-dev \
    && curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/11/prod bullseye main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 libgssapi-krb5-2 \
    && rm -rf /var/lib/apt/lists/*

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
