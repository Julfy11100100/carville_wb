import jwt

from config import settings
from app.schemas.auth import CheckTokenResponse


def is_valid_token(token: str) -> CheckTokenResponse:
    """
    Проверка токена на валидность
    """

    result = CheckTokenResponse(
        status="success",
        api_key_valid=True,
        permissions_valid=True
    )

    # Для работы с тестовыми
    if settings.DEBUG:
        return result

    try:
        # Декодируем без проверки подписи, чтобы прочитать payload
        payload = jwt.decode(token, options={"verify_signature": False})
        s = payload.get('s', 0)

        # Проверяем доступ к категории Контент (1-й бит)
        result.api_key_valid = bool(s & (1 << 1))

        # Проверяем тип доступа (30-й бит = только чтение) должен быть не только чтение, поэтому not
        result.permissions_valid = not bool(s & (1 << 30))

        return result
    except jwt.exceptions.DecodeError:
        return CheckTokenResponse(
            status="error",
            api_key_valid=False,
            permissions_valid=False
        )
