import re

NORMALIZE_PATTERN = re.compile(r'[^a-zA-ZА-Яа-я0-9]')


def get_normalize_identifier(value: str) -> str:
    """Нормализация строки

    Удаляет все символы кроме букв (латиница и кириллица) и цифр.
    Соответствует логике SQL процедуры: [dbo].[NormalizeString](val, '%[^a-zA-Zа-ЯА-Я0-9]%')

    Args:
        value: Строка для нормализации

    Returns:
        Строка содержащая только буквы и цифры
    """
    return NORMALIZE_PATTERN.sub('', str(value))
