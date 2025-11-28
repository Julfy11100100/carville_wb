import re
from typing import List, Union, Optional, Dict

from pydantic import BaseModel, Field, field_validator

from app.constants.validators import VALID_WB_FIELDS, VALID_CARVILLE_FIELDS, VALID_COMPARISON_FIELDS, \
    VALID_WB_FIELD_PATTERN


class ProductMatchRequest(BaseModel):
    """Запрос на сопоставление товаров"""

    wb_match_field: str = Field(...,
                                description="Поле по которому будем матчить полученные товары из API с нашими")
    carville_match_field: str = Field(..., description="Поле в нашей БД с которым будем матчить продукты")
    comparison_field: str = Field(..., description="Имя поля которое будем сравнивать у сматченных объектов")
    brand: Optional[int] = Field(None, description="Бренд для фильтрации")
    filter: Dict[str, List[int]] = Field(...,
                                         description="Фильтр по категориям: ключ - ID родительской категории (строка), значение - список подкатегорий (может быть пустым для всех типов категории)")

    @field_validator('wb_match_field')
    def validate_wb_match_field(cls, v):
        match = re.match(VALID_WB_FIELD_PATTERN, v)
        if v not in VALID_WB_FIELDS and not match:
            raise ValueError(
                f"Недопустимое wb_match_field '{v}' Допустимые поля {','.join(VALID_WB_FIELDS)} "
                f"Либо должен соответствовать паттерну '{VALID_WB_FIELD_PATTERN}'")
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

    @field_validator('filter')
    def validate_filter(cls, v: Dict[str, List[int]]) -> Dict[str, List[int]]:
        # Проверка, что словарь не пустой
        if not v:
            raise ValueError('Фильтр не может быть пустым')

        # Проверка всех значений в списках
        for parent_category_id, subcategories in v.items():

            try:
                if int(parent_category_id) < 0:
                    raise ValueError(
                        f"Родительская категория должна быть больше 0. "
                        f"Найдено некорректное значение родительской категории {parent_category_id}"
                    )
            except ValueError:
                raise ValueError(
                    f"Id родительской категории должно быть числом, получено: {parent_category_id}"
                )

            # Если список подкатегорий не пустой, проверяем значения
            if subcategories:
                for subcategory_id in subcategories:
                    if subcategory_id <= 0:
                        raise ValueError(
                            f"Все ID подкатегорий должны быть больше 0. "
                            f"Найдено некорректное значение {subcategory_id} в категории '{parent_category_id}'"
                        )

        return v


class ProductMatchResponse(BaseModel):
    """Ответ с информацией о сопоставленном товаре"""
    nm_id: Union[str, int] = Field(..., description="ID товара в системе продавца (SKU)")
    identifier_value: Union[str, int] = Field(..., description="Значение идентификатора для сопоставления")
    carville_value: Union[str, int, list[int]] = Field(..., description="Значение поля в базе данных Carville")
    wb_value: Union[str, int] = Field(..., description="Значение поля в WB")
    recommend_type: Optional[str] = Field(None, description="Поле Recommend_name_type из БД")


class ProductListMatchResponse(ProductMatchRequest):
    """Ответ с результатами сопоставления товаров"""
    products: List[ProductMatchResponse] = Field(..., description="Список сопоставленных товаров")
    total_products: int = Field(...,
                                description="Сколько товаров всего получено из сохраненных по API после фильтрации")
    matched_products: int = Field(..., description="Сколько товаров осталось после матчинга с результатами БД")
