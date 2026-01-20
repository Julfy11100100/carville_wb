from typing import Dict, Any, Optional, Callable, Awaitable

import aiohttp

from app.constants.wb_api import ResponseStatus
from app.exceptions.wb_api import WildberriesAPIError
from app.utils.logging import get_logger

logger = get_logger()


class WBErrorHandler:
    """Обработчик ошибок Wildberries API."""

    RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}

    @staticmethod
    def extract_wb_error_data(
            e: WildberriesAPIError | aiohttp.ClientError | Exception,
            status_code: Optional[int] = None,
            response_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Извлекает структурированные данные ошибки из ответа WB API."""
        error_message = str(e)
        error_data = {
            "error": error_message,
            "status_code": status_code,
            "code": None,
            "details": None,
            "message": None
        }

        if isinstance(e, WildberriesAPIError):
            error_data["status_code"] = status_code or getattr(e, 'status_code', None)

            if response_data is None:
                response_data = getattr(e, 'response_data', None)

        if response_data and isinstance(response_data, dict):
            error_text = response_data.get("errorText")
            error_message = response_data.get("error")
            details = response_data.get("details")
            code = response_data.get("code")
            message = response_data.get("message")

            if code:
                error_data["code"] = code
            if message:
                error_data["message"] = message
            if details:
                error_data["details"] = details

            error_message = (
                    error_text
                    or message
                    or error_message
                    or (str(details) if details else None)
                    or error_message
            )

        error_data["error"] = error_message
        return error_data

    @staticmethod
    def extract_error_message(
            e: WildberriesAPIError | aiohttp.ClientError | Exception,
            status_code: Optional[int] = None,
            response_data: Optional[Dict[str, Any]] = None
    ) -> str:
        """Извлекает читаемое сообщение об ошибке."""
        error_data = WBErrorHandler.extract_wb_error_data(e, status_code, response_data)
        return error_data.get("error", str(e))

    @staticmethod
    def create_error_response(
            e: WildberriesAPIError | aiohttp.ClientError | Exception,
            status_code: Optional[int] = None,
            response_data: Optional[Dict[str, Any]] = None,
            **extra_fields
    ) -> Dict[str, Any]:
        """Создаёт стандартизированный ответ с ошибкой."""
        error_data = WBErrorHandler.extract_wb_error_data(e, status_code, response_data)
        return {
            "status": ResponseStatus.ERROR,
            **error_data,
            **extra_fields
        }

    @staticmethod
    def create_success_response(**fields) -> Dict[str, Any]:
        """Создаёт стандартизированный успешный ответ."""
        return {
            "status": ResponseStatus.SUCCESS,
            **fields
        }

    @staticmethod
    async def safe_api_call(
            operation_name: str,
            api_call: Callable[[], Awaitable[Dict[str, Any]]],
            success_transform: Optional[Callable[[Dict], Dict]] = None,
            default_error_fields: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Безопасный вызов API с обработкой ошибок."""
        try:
            result = await api_call()
            return (
                success_transform(result)
                if success_transform
                else WBErrorHandler.create_success_response(**result)
            )
        except WildberriesAPIError as e:
            status_code = getattr(e, 'status_code', None)
            response_data = getattr(e, 'response_data', None)

            # ВАЖНО: для retryable статусов - пробрасываем исключение
            # чтобы RetryService мог обработать retry
            if status_code in WBErrorHandler.RETRYABLE_STATUS_CODES:
                logger.debug(
                    f"{operation_name} got retryable status {status_code}, "
                    f"re-raising for retry"
                )
                raise  # Пробрасываем для retry

            logger.error(
                f"{operation_name} failed: WB API Error {status_code} "
                f"error_data: {response_data}",
            )
            return WBErrorHandler.create_error_response(
                e,
                status_code=status_code,
                response_data=response_data,
                **(default_error_fields or {})
            )
        except aiohttp.ClientError as e:
            logger.error(f"{operation_name} failed: Network error - {str(e)}")
            return {
                "status": ResponseStatus.ERROR,
                "error": f"Network error: {str(e)}",
                "code": "network_error",
                **(default_error_fields or {})
            }
        except Exception as e:
            logger.error(f"{operation_name} failed: {str(e)}", exc_info=True)
            return {
                "status": ResponseStatus.ERROR,
                "error": str(e),
                "code": "internal_error",
                **(default_error_fields or {})
            }
