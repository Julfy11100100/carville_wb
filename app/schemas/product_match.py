from typing import List, Union, Optional
from pydantic import BaseModel, Field, field_validator
from app.constants.validators import VALID_WB_FIELDS, VALID_CARVILLE_FIELDS, VALID_COMPARISON_FIELDS


class ProductMatchRequest(BaseModel):
    """Запрос на сопоставление товаров"""

    wb_match_field: str = Field(...,
                                description="Поле по которому будем матчить полученные товары из API с нашими")
    carville_match_field: str = Field(..., description="Поле в нашей БД с которым будем матчить продукты")
    root_category_id: int = Field(...,
                             description="ID родительской категории товаров которые будем матчить (берём все дочернии)")
    category_ids: Optional[List[int]] = Field(None, description="Список категорий, необязательный параметр")
    comparison_field: str = Field(..., description="Имя поля которое будем сравнивать у сматченных объектов")
    brand: Optional[int] = Field(None, description="Бренд для фильтрации")

    @field_validator('wb_match_field')
    def validate_wb_match_field(cls, v):
        if v not in VALID_WB_FIELDS:
            raise ValueError(
                f"Недопустимое wb_match_field '{v}' Допустимые поля {','.join(VALID_WB_FIELDS)}")
        return v

    @field_validator('carville_match_field')
    def validate_carville_match_field(cls, v):
        if v not in VALID_CARVILLE_FIELDS:
            raise ValueError(
                f"Недопустимое carville_match_field '{v}' Допустимые поля {','.join(VALID_CARVILLE_FIELDS)}")
        return v

    @field_validator('comparison_field')
    def validate_comparison_field(cls, v):
        if v not in VALID_COMPARISON_FIELDS:
            raise ValueError(
                f"Недопустимое comparison_field '{v}' Допустимые поля {','.join(VALID_COMPARISON_FIELDS)}")
        return v

    @field_validator('root_category_id')
    def validate_root_category_id(cls, v):
        if v < 0:
            raise ValueError(f"Поле root_category_id не может быть отрицательным")
        return v

    @field_validator('category_ids')
    def validate_category_ids(cls, v):
        if v is None:
            return v
        for category in v:
            if category < 0:
                raise ValueError(f"Поле category в category_ids не может быть отрицательным")
        return v

class ProductMatchResponse(BaseModel):
    """Ответ с информацией о сопоставленном товаре"""
    nm_id: Union[str, int] = Field(..., description="ID товара в системе продавца (SKU)")
    identifier_value: Union[str, int] = Field(..., description="Значение идентификатора для сопоставления")
    carville_value: Union[str, int, list[int]] = Field(..., description="Значение поля в базе данных Carville")
    wb_value: Union[str, int] = Field(..., description="Значение поля в WB")


class ProductListMatchResponse(ProductMatchRequest):
    """Ответ с результатами сопоставления товаров"""
    products: List[ProductMatchResponse] = Field(..., description="Список сопоставленных товаров")
    total_products: int = Field(...,
                                description="Сколько товаров всего получено из сохраненных по API после фильтрации")
    matched_products: int = Field(..., description="Сколько товаров осталось после матчинга с результатами БД")
