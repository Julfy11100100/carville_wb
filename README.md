# Carville WB Manager

**Асинхронный FastAPI сервис для управления товарами на WildBerries**

Проект предоставляет REST API для интеграции с WildBerries API, сбора информации о товарах, их сопоставления и массового обновления.

---

## 📋 Содержание

- [Возможности](#возможности)
- [Требования](#требования)
- [Установка](#установка)
- [Конфигурация](#конфигурация)
- [Запуск](#запуск)
- [API Документация](#api-документация)

---

## ✨ Возможности

### Аутентификация
- ✅ Проверка валидности WildBerries API токена

### Управление товарами
- ✅ **Сбор товаров** — асинхронное получение всех товаров с сохранением в файл и индексацией
- ✅ **Обновление товаров** — массовое обновление с проверкой результатов
- ✅ **Сопоставление товаров** — матчинг товаров WB с товарами из БД по указанным полям

### Отслеживание задач
- ✅ Асинхронные фоновые задачи с отслеживанием прогресса
- ✅ Проверка статуса задач с фильтрацией по типу и периоду

### Категории
- ✅ Получение полного иерархического дерева категорий WildBerries

---

## 🔧 Требования

- **Python**: 3.10+
- **Зависимости**: см. [requirements.txt](#зависимости)
- **Сервисы**:
  - MongoDB 7.0+ (для хранения задач и категорий)
  - Elasticsearch 8.11+ (для полнотекстового поиска товаров)
  - Можно поднять через docker compose docker-compose_dev.yaml

---

## 📦 Установка

### Локальная установка

1. **Клонируй репозиторий**
   ```bash
   git clone https://github.com/Julfy11100100/carville_wb.git
   cd carville_wb
   git checkout FRE-1
   ```

2. **Создай виртуальное окружение**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Linux/macOS
   .venv\Scripts\activate     # Windows
   ```

3. **Установи зависимости**
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

4. **Настрой переменные окружения**
   ```bash
   cp .env.example .env
   # Отредактируй .env
   ```

### Docker установка

```bash
docker compose up -d --build wb_manager
```

### Запуск через Docker Compose

```bash
# Запуск в фоне
docker compose up -d wb_manager

# Запуск с выводом логов
docker compose up wb_manager

# Перестартовать контейнер при изменении кода
docker compose restart wb_manager

# Посмотреть логи
docker compose logs -f wb_manager
```

### Проверка здоровья сервиса

```bash
curl http://localhost:8000/api/health
```

Ответ:
```json
{
  "status": "ok",
  "timestamp": "2025-11-10T10:00:00+00:00"
}
```

---

## 📚 API Документация

### OpenAPI (Swagger UI)

После запуска приложения, OpenAPI документация доступна по адресу:

```
http://localhost:8000/api/docs
```

(если используется `root_path="/api"`, иначе `http://localhost:8000/docs`)
