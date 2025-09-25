from typing import Dict


class WildberriesAPIError(Exception):
    """Базовое исключение для ошибок Wildberries API"""

    def __init__(self, message: str, status_code: int = None, response_data: Dict = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data or {}


class RateLimitError(WildberriesAPIError):
    """Исключение для ошибок превышения лимитов"""

    def __init__(self, message: str, retry_after: float = None, **kwargs):
        super().__init__(message, **kwargs)
        self.retry_after = retry_after
