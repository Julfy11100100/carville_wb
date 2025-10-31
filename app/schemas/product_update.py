from typing import List, Union

from pydantic import BaseModel, Field, field_validator
from pydantic_core.core_schema import ValidationInfo
from app.constants.validators import ALLOWED_FIELDS_FOR_UPDATES


class ProductUpdateItem(BaseModel):
    """Элемент для обновления продукта"""
    nm_id: Union[int, str] = Field(..., description="ID товара в системе продавца")
    value: Union[int, str, float, list, dict] = Field(..., description="Новое значение поля")


class ProductUpdateRequest(BaseModel):
    """Запрос на обновление продуктов"""
    update_field: str = Field(..., min_length=1, description="Поле для обновления")
    products: List[ProductUpdateItem] = Field(..., min_length=1, description="Список продуктов для обновления")

    @field_validator('update_field')
    def validate_field(cls, v):
        if v not in ALLOWED_FIELDS_FOR_UPDATES:
            raise ValueError(
                f"Недопустимое update_field '{v}' Допустимые поля {','.join(ALLOWED_FIELDS_FOR_UPDATES.keys())}")
        return v

    @field_validator('products', mode='after')
    def validate_products(cls, v, info: ValidationInfo):
        # Уникальность
        if len(set(str(p.offer_id) for p in v)) != len(v):
            raise ValueError("offer_id должны быть уникальными")

        # Типы значений
        field_name = info.data.get('update_field')
        expected_type = ALLOWED_FIELDS_FOR_UPDATES.get(field_name)

        for idx, product in enumerate(v):
            if not isinstance(product.value, expected_type):
                raise ValueError(
                    f"Продукт #{idx}: ожидается {expected_type.__name__}, "
                    f"получен {type(product.value).__name__}"
                )

            # Специфика type_id
            if field_name == "type_id" and not (
                    isinstance(product.value, list) and
                    len(product.value) == 2 and
                    all(isinstance(x, int) for x in product.value)
            ):
                raise ValueError(f"Продукт #{idx}: type_id должен быть [int, int]")

        return v


class ProductUpdateResponse(BaseModel):
    """Ответ на запрос обновления продуктов"""
    comparison_field: str = Field(..., description="Поле для сравнения")
    total_products_to_update: int = Field(..., description="Количество продуктов для обновления")


class ProductUpdateErrorResponse(BaseModel):
    """Ответ с ошибкой при обновлении продуктов"""
    error: str = Field(..., description="Описание ошибки")
    details: str = Field(..., description="Детали ошибки")
