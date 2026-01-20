class ResponseStatus:
    """Статусы ответов API."""
    SUCCESS = "success"
    ERROR = "error"


class WBErrorCodes:
    """Коды ошибок WB API."""
    RATE_LIMIT = "rate_limit"
    VALIDATION_ERROR = "validation_error"
    NOT_FOUND = "not_found"
    UNAUTHORIZED = "unauthorized"
    INTERNAL_ERROR = "internal_error"
