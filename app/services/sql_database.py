from contextlib import asynccontextmanager
from typing import AsyncGenerator

import aioodbc

from app.exceptions.sql_database import DatabaseError
from app.services.base_reconnectable import ReconnectableService
from app.utils.logging import get_logger
from config import settings

logger = get_logger()


class SQLDatabaseService(ReconnectableService):

    def __init__(self):
        super().__init__("Database")
        self.connection_string = self._build_connection_string()
        self._pool = None

    def _build_connection_string(self) -> str:
        """Формирует строку подключения к MSSQL"""
        return (
            f"Driver={settings.DB_DRIVER};"
            f"Server={settings.DB_SERVER},{settings.DB_PORT};"
            f"Database={settings.DB_NAME};"
            f"UID={settings.DB_USER};"
            f"PWD={settings.DB_PASSWORD};"
            "Encrypt=yes;"
            "TrustServerCertificate=yes;"
            "Connection Timeout=30;"
        )

    async def _connect(self):
        """Создает пул соединений"""
        if self._pool:
            await self._disconnect()

        self._pool = await aioodbc.create_pool(
            dsn=self.connection_string,
            minsize=1,
            maxsize=10,  # Захардкожено как было в конфиге
            timeout=30
        )
        logger.info("Database pool created successfully")

    async def _disconnect(self):
        """Закрывает пул соединений"""
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None

    async def _health_check(self) -> bool:
        """Проверка здоровья БД"""
        if not self._pool:
            return False

        try:
            async with self._pool.acquire() as conn:
                # Устанавливаем короткий LOCK_TIMEOUT для health check (1 секунда)
                try:
                    async with conn.cursor() as timeout_cursor:
                        await timeout_cursor.execute("SET LOCK_TIMEOUT 1000")
                except Exception:
                    pass  # Игнорируем ошибку установки timeout

                async with conn.cursor() as cursor:
                    # Проверяем базовое соединение к БД
                    # Не проверяем конкретные таблицы - блокировки таблиц это временная ситуация,
                    # которая не означает что БД недоступна в целом
                    await cursor.execute("SELECT 1")
                    result = await cursor.fetchone()
                    return result[0] == 1 if result else False
        except Exception as e:
            # Логируем timeout/lock ошибки как warning, не как error
            error_msg = str(e).lower()
            timeout_indicators = ['timeout', 'lock', 'deadlock', 'blocked', 'превышено', 'блокировк', '1222', '1205']
            if any(indicator in error_msg for indicator in timeout_indicators):
                logger.warning(f"Database health check blocked (possible long transaction): {str(e)}")
            return False

    # Совместимость со старым API
    async def create_pool(self):
        """Создает пул соединений (для совместимости)"""
        await self._connect()
        self._is_connected = True

    async def close_pool(self):
        """Закрывает пул соединений (для совместимости)"""
        await self._disconnect()
        self._is_connected = False

    @asynccontextmanager
    async def get_connection(self, query_timeout: int = 5) -> AsyncGenerator[aioodbc.Connection, None]:
        """Контекстный менеджер для получения соединения

        Args:
            query_timeout: Таймаут выполнения запроса в секундах (по умолчанию 5 секунд)
        """
        try:
            await self.ensure_connection()
        except RuntimeError as e:
            # Circuit breaker открыт
            logger.warning(f"Database circuit breaker is open: {str(e)}")
            # Запускаем фоновое переподключение
            self._schedule_background_reconnect()
            raise DatabaseError("Database service temporarily unavailable - circuit breaker open")
        except Exception as e:
            logger.error(f"Failed to ensure database connection: {str(e)}")
            self._record_failure()
            self._schedule_background_reconnect()
            raise DatabaseError(f"Failed to connect to database: {str(e)}")

        try:
            async with self._pool.acquire() as conn:
                # Устанавливаем LOCK_TIMEOUT для защиты от длинных блокировок
                # query_timeout в секундах -> миллисекунды для SQL Server
                lock_timeout_ms = query_timeout * 1000
                try:
                    async with conn.cursor() as cursor:
                        await cursor.execute(f"SET LOCK_TIMEOUT {lock_timeout_ms}")
                except Exception as timeout_set_error:
                    logger.warning(f"Failed to set LOCK_TIMEOUT: {str(timeout_set_error)}")

                yield conn
        except Exception as e:
            error_msg = str(e).lower()

            # Проверяем, является ли это timeout/lock ошибкой
            # SQL Server коды: 1222 - lock timeout, 1205 - deadlock
            # Проверяем и английские слова, и русские, и коды ошибок
            timeout_indicators = [
                'timeout', 'lock', 'deadlock', 'blocked',  # Английские
                'превышено', 'блокировк', 'ожидания',  # Русские
                '1222', '1205'  # SQL Server error codes
            ]

            if any(indicator in error_msg for indicator in timeout_indicators):
                logger.warning(f"Database query timeout or lock detected: {str(e)}")
                # Не записываем failure для circuit breaker - это временная проблема
                # И не запускаем background reconnection
                raise DatabaseError(
                    "Database temporarily unavailable - query timeout (possible long-running transaction)")

            logger.error(f"Database connection error: {str(e)}")
            # Записываем ошибку для circuit breaker только для реальных проблем
            self._record_failure()
            # Запускаем фоновое переподключение
            self._schedule_background_reconnect()
            raise DatabaseError(f"Database operation failed: {str(e)}")

