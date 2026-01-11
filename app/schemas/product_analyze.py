import re
from typing import List, Optional, Dict

from pydantic import Field, BaseModel, field_validator

from app.constants.brands import BRANDS
from app.constants.validators import VALID_ANALYZE_PATTERN, VALID_ANALYZE_FIELDS


class ProductAnalyzeRequest(BaseModel):
    """Запрос на анализ данных товаров"""

    brand: str = Field(..., description="Бренд для анализа")
    carville_match_field: str = Field(..., description="Наше поле на WB для сравнения")
    client_match_field: str = Field(..., description="Поле клиента на WB для сравнения")

    @field_validator('carville_match_field')
    def validate_carville_match_field(cls, v):
        if v not in VALID_ANALYZE_FIELDS:
            raise ValueError(
                f"Недопустимое carville_match_field '{v}' Допустимые поля {', '.join(VALID_ANALYZE_FIELDS)}")
        return v

    @field_validator('client_match_field')
    def validate_wb_match_field(cls, v):
        match = re.match(VALID_ANALYZE_PATTERN, v)
        if v not in VALID_ANALYZE_FIELDS and not match:
            raise ValueError(
                f"Недопустимое client_match_field '{v}' Допустимые поля {', '.join(VALID_ANALYZE_FIELDS)} "
                f"Либо должен соответствовать паттерну '{VALID_ANALYZE_PATTERN}'")
        return v

    @field_validator('brand')
    def validate_brand(cls, v):
        allowed_brands_lower = [b.strip().lower() for b in BRANDS]
        if v.strip().lower() not in allowed_brands_lower:
            raise ValueError(
                f"Недопустимое brand '{v}' Допустимые поля {', '.join(BRANDS)} "
            )
        return v


class ProductAnalyzeItem(BaseModel):
    """Модель товара для анализа"""
    vendor_code: str = Field(..., description="ID товара в системе продавца")
    type_id: int = Field(..., description="ID типа товара")
    oem: List[str] = Field(default_factory=list, description="OEM-номера товара")
    cross: List[str] = Field(default_factory=list, description="Альтернативные артикулы товара")
    name: Optional[str] = Field(..., description="Название товара")


class ProductAnalyzeResponse(BaseModel):
    """Ответ с результатами анализа товаров"""

    products: List[ProductAnalyzeItem] = Field(..., description="Список товаров для анализа")
    category_ids: Optional[Dict[str, List[int]]] = Field(
        None,
        description="Словарь {category_id: [type_id, ...]} с уникальными type_id для каждой категории (только для products_info)"
    )
    total_products: Optional[int] = Field(..., description="Общее количество собранных товара")
