import asyncio
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Optional

from app.utils.logging import get_logger

logger = get_logger()


class ReconnectableService(ABC):
    """Базовый класс для сервисов с переподключением"""

    def __init__(self, service_name: str):
        self.service_name = service_name
        self._is_connected = False
        self._reconnection_lock = asyncio.Lock()
        self._is_reconnecting = False

        # Circuit breaker настройки
        self._failed_attempts = 0
        self._max_failed_attempts = 5
        self._circuit_open_until: Optional[datetime] = None
        self._circuit_break_duration = timedelta(minutes=5)

    @abstractmethod
    async def _connect(self) -> None:
        """Реализация подключения к сервису"""
        pass

    @abstractmethod
    async def _disconnect(self) -> None:
        """Реализация отключения от сервиса"""
        pass

    @abstractmethod
    async def _health_check(self) -> bool:
        """Реализация проверки здоровья сервиса"""
        pass

    @property
    def is_connected(self) -> bool:
        """Проверка состояния подключения"""
        return self._is_connected

    async def ensure_connection(self) -> None:
        """Обеспечивает активное соединение"""
        if self._is_circuit_open():
            raise RuntimeError(f"{self.service_name} circuit breaker is open")

        if not self._is_connected:
            async with self._reconnection_lock:
                if not self._is_connected:
                    logger.warning(f"Attempting to reconnect to {self.service_name}...")
                    await self._reconnect_with_strategy()

    async def health_check(self) -> bool:
        """Проверка здоровья с circuit breaker"""
        try:
            if self._is_circuit_open():
                return False

            if not self._is_connected:
                return False

            result = await self._health_check()
            if result:
                self._reset_circuit_breaker()
            else:
                self._record_failure()

            return result

        except Exception as e:
            logger.error(f"{self.service_name} health check failed: {str(e)}")
            self._record_failure()
            return False

    async def background_reconnect(self) -> None:
        """Фоновое переподключение без блокировки"""
        if self._is_reconnecting or self._is_circuit_open():
            return

        self._is_reconnecting = True
        try:
            logger.info(f"Starting background reconnection to {self.service_name}...")
            await self._reconnect_with_strategy()
            logger.info(f"Background reconnection to {self.service_name} completed")
        except Exception as e:
            logger.error(f"Background reconnection to {self.service_name} failed: {str(e)}")
        finally:
            self._is_reconnecting = False

    async def _reconnect_with_strategy(self) -> None:
        """Переподключение с многоуровневой стратегией"""
        intervals = [30, 120, 300, 600]  # 30с, 2мин, 5мин, 10мин
        max_interval = 600  # максимум 10 минут

        wave = 0
        while not self._is_connected:
            # 3 попытки с интервалом 5 сек
            for attempt in range(3):
                try:
                    await self._connect()
                    self._is_connected = True
                    self._reset_circuit_breaker()
                    logger.info(f"Successfully reconnected to {self.service_name}")
                    return
                except Exception as e:
                    logger.error(f"{self.service_name} reconnection attempt {attempt + 1}/3 failed: {str(e)}")
                    if attempt < 2:
                        await asyncio.sleep(5)

            # Если волна неуспешна, ждем и переходим к следующей
            if wave < len(intervals):
                wait_time = intervals[wave]
                wave += 1
            else:
                wait_time = max_interval

            logger.info(f"Next {self.service_name} reconnection wave in {wait_time} seconds...")
            await asyncio.sleep(wait_time)

    def _is_circuit_open(self) -> bool:
        """Проверка состояния circuit breaker"""
        if self._circuit_open_until is None:
            return False

        if datetime.now() >= self._circuit_open_until:
            self._circuit_open_until = None
            logger.info(f"{self.service_name} circuit breaker closed, resuming operations")
            return False

        return True

    def _record_failure(self) -> None:
        """Записать неудачную попытку"""
        self._failed_attempts += 1
        self._is_connected = False

        if self._failed_attempts >= self._max_failed_attempts:
            self._circuit_open_until = datetime.now() + self._circuit_break_duration
            logger.warning(f"{self.service_name} circuit breaker opened for {self._circuit_break_duration}")

    def _reset_circuit_breaker(self) -> None:
        """Сбросить circuit breaker"""
        if self._failed_attempts > 0:
            logger.info(f"{self.service_name} circuit breaker reset")
        self._failed_attempts = 0
        self._circuit_open_until = None

    def _schedule_background_reconnect(self) -> None:
        """Безопасное планирование фонового переподключения"""
        try:
            # Получаем текущий event loop
            loop = asyncio.get_running_loop()
            # Создаем задачу в текущем loop
            loop.create_task(self.background_reconnect())
        except RuntimeError:
            # Если нет активного loop, просто логируем
            logger.warning(f"Cannot schedule background reconnect for {self.service_name} - no active event loop")

    async def disconnect(self) -> None:
        """Отключение от сервиса"""
        try:
            await self._disconnect()
            self._is_connected = False
            logger.info(f"Disconnected from {self.service_name}")
        except Exception as e:
            logger.error(f"Error during {self.service_name} disconnection: {str(e)}")


class RetryService:
    """Сервис для выполнения операций с повторными попытками"""

    @staticmethod
    async def execute_with_retry(
            operation,
            max_retries: int = 3,
            base_delay: float = 1.0,
            exponential_backoff: bool = True,
            operation_name: str = "operation"
    ):
        """
        Выполняет операцию с повторными попытками

        Args:
            operation: Асинхронная функция для выполнения
            max_retries: Максимальное количество попыток
            base_delay: Базовая задержка между попытками
            exponential_backoff: Использовать экспоненциальную задержку
            operation_name: Название операции для логирования
        """
        last_error = None

        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    delay = base_delay * (2 ** (attempt - 1)) if exponential_backoff else base_delay
                    logger.info(f"Retry {operation_name} attempt {attempt + 1}/{max_retries} after {delay}s delay")
                    await asyncio.sleep(delay)

                return await operation()

            except Exception as e:
                last_error = e
                error_msg = str(e)

                # Определяем, стоит ли повторять попытку
                if RetryService._should_retry(error_msg):
                    logger.warning(f"{operation_name} attempt {attempt + 1}/{max_retries} failed: {error_msg}")
                    if attempt < max_retries - 1:
                        continue
                else:
                    logger.error(f"{operation_name} failed with non-retryable error: {error_msg}")
                    break

        logger.error(f"{operation_name} failed after {max_retries} attempts: {str(last_error)}")
        raise last_error

    @staticmethod
    def _should_retry(error_message: str) -> bool:
        """Определяет, стоит ли повторять операцию на основе ошибки"""
        # Ошибки, при которых стоит повторить попытку
        retryable_errors = [
            "timeout",
            "connection",
            "network",
            "temporary failure",
            "name resolution",
            "dns"
        ]

        error_lower = error_message.lower()
        return any(retryable_error in error_lower for retryable_error in retryable_errors)
