from typing import List, Union, Optional

from pydantic import BaseModel, Field


class ProductMatchRequest(BaseModel):
    """Запрос на сопоставление товаров"""

    wb_match_field: str = Field(...,
                                description="Поле по которому будем матчить полученные товары из API с нашими")
    carville_match_field: str = Field(..., description="Поле в нашей БД с которым будем матчить продукты")
    category_id: int = Field(..., description="ID категории товаров которые будем матчить")
    comparison_field: str = Field(..., description="Имя поля которое будем сравнивать у сматченных объектов")
    brand: Optional[int] = Field(None, description="Бренд для фильтрации")


class ProductMatchResponse(BaseModel):
    """Ответ с информацией о сопоставленном товаре"""
    offer_id: Union[str, int] = Field(..., description="ID товара в системе продавца (SKU)")
    identifier_value: Union[str, int] = Field(..., description="Значение идентификатора для сопоставления")
    carville_value: Union[str, int, list[int]] = Field(..., description="Значение поля в базе данных Carville")
    wb_value: Union[str, int] = Field(..., description="Значение поля в")


class ProductListMatchResponse(ProductMatchRequest):
    """Ответ с результатами сопоставления товаров"""
    products: List[ProductMatchResponse] = Field(..., description="Список сопоставленных товаров")
    total_products: int = Field(...,
                                description="Сколько товаров всего получено из сохраненных по API после фильтрации")
    matched_products: int = Field(..., description="Сколько товаров осталось после матчинга с результатами БД")
