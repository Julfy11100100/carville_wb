from typing import List, Optional, Union

from pydantic import Field, BaseModel


class ProductCreateItem(BaseModel):
    """Элемент списка товаров для создания"""
    vendor_code: str = Field(..., description="ID товара")


class ProductCreateRequest(BaseModel):
    """Запрос на создание товаров"""
    products: List[ProductCreateItem] = Field(..., description="Список товаров для создания", min_length=1)
    brand: Optional[str] = Field(None, description="Бренд товаров (опционально)")
