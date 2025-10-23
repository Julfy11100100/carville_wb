from typing import Optional, Dict


class WildberriesAPIError(Exception):
    """Базовое исключение для ошибок WB API"""

    def __init__(self, message: str, status_code: Optional[int] = None, response_data: Optional[Dict] = None):
        self.message = message
        self.status_code = status_code
        self.response_data = response_data
        super().__init__(self.message)


class WildberriesRateLimitError(WildberriesAPIError):
    """Исключение для ошибок превышения rate limit"""
    pass