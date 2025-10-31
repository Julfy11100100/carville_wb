import jwt

from config import settings


def is_valid_token(token: str):
    """
    Проверка токена на валидность
    """

    # Для работы с
    if settings.DEBUG:
        return True

    try:
        # Декодируем без проверки подписи, чтобы прочитать payload
        payload = jwt.decode(token, options={"verify_signature": False})
        s = payload.get('s', 0)

        # Проверяем доступ к категории Контент (1-й бит)
        has_content_access = bool(s & (1 << 1))

        # Проверяем тип доступа (30-й бит = только чтение)
        is_read_only = bool(s & (1 << 30))

        return has_content_access and not is_read_only
    except jwt.exceptions.DecodeError:
        return False
