import asyncio
from typing import Callable, Optional, Set

from app.utils.logging import get_logger

logger = get_logger()


class RetryService:
    """Сервис для выполнения операций с повторными попытками."""

    @staticmethod
    async def execute_with_retry(
            operation: Callable,
            max_retries: int = 3,
            base_delay: float = 1.0,
            exponential_backoff: bool = True,
            operation_name: str = "operation",
            retryable_statuses: Optional[Set[int]] = None
    ):
        """Выполняет операцию с повторными попытками при ошибках.

        Для 4xx ошибок — raise БЕЗ traceback.
        Для 5xx и сетевых — retry с логированием.
        """
        if retryable_statuses is None:
            retryable_statuses = {408, 429, 500, 502, 503, 504}

        last_error = None
        last_status_code = None

        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    delay = base_delay * (2 ** (attempt - 1)) if exponential_backoff else base_delay
                    logger.info(
                        f"Retry {operation_name} attempt {attempt + 1}/{max_retries} "
                        f"after {delay:.1f}s delay"
                    )
                    await asyncio.sleep(delay)

                return await operation()

            except Exception as e:
                last_error = e
                last_status_code = RetryService._extract_status_code(e)
                error_msg = str(e)
                is_client_error = (last_status_code and 400 <= last_status_code < 500)

                # КЛЮЧЕВОЕ: для 4xx — raise БЕЗ обработки traceback
                if is_client_error:
                    # Suppress traceback and raise immediately
                    raise e.with_traceback(None)

                # Для остальных ошибок — логируем и retry
                should_retry = RetryService._should_retry(
                    error_msg,
                    last_status_code,
                    retryable_statuses
                )

                if should_retry:
                    logger.warning(
                        f"{operation_name} attempt {attempt + 1}/{max_retries} failed: "
                        f"status={last_status_code}, retrying..."
                    )
                    if attempt < max_retries - 1:
                        continue

                # Финальная ошибка
                logger.warning(
                    f"{operation_name} attempt {attempt + 1}/{max_retries} failed: "
                    f"status={last_status_code}"
                )

                if attempt == max_retries - 1:
                    if not last_status_code:
                        logger.error(
                            f"{operation_name} failed after {max_retries} attempts: {error_msg}",
                            exc_info=True
                        )
                    break

        raise last_error

    @staticmethod
    def _extract_status_code(exception: Exception) -> Optional[int]:
        """Извлекает HTTP статус код из исключения."""
        for attr in ['status_code', 'status', 'code']:
            if hasattr(exception, attr):
                value = getattr(exception, attr)
                if isinstance(value, int):
                    return value
        return None

    @staticmethod
    def _should_retry(
            error_message: str,
            status_code: Optional[int] = None,
            retryable_statuses: Optional[Set[int]] = None
    ) -> bool:
        """Определяет возможность retry на основе ошибки и статуса."""
        if retryable_statuses is None:
            retryable_statuses = {408, 429, 500, 502, 503, 504}

        if status_code and status_code in retryable_statuses:
            return True

        if status_code and 400 <= status_code < 500:
            return False

        retryable_errors = [
            "timeout", "connection", "network", "temporary failure",
            "name resolution", "dns", "reset by peer", "broken pipe",
            "connection refused",
        ]

        error_lower = error_message.lower()
        return any(retryable_error in error_lower for retryable_error in retryable_errors)
