from typing import List

from pydantic import Field, BaseModel


class WbTypeResponse(BaseModel):
    """Модель для типа товара"""
    type_id: int = Field(..., description="Уникальный ID типа товара в Wb")
    name: str = Field(..., description="Название типа товара")
    carville_cat: bool = Field(..., description="Флаг категории Carville (true если категория относится к Carville)")


class CategoryResponse(BaseModel):
    """Модель для корневой категории"""
    type_id: int = Field(..., description="Уникальный ID корневой категории в Wb")
    name: str = Field(..., description="Название корневой категории")
    categories: List[WbTypeResponse] = Field(..., description="Список подкатегорий в этой корневой категории")


class WbTypesTreeResponse(BaseModel):
    """Модель для дерева категорий Wb"""
    root_categories: List[CategoryResponse] = Field(...,
                                                    description="Список корневых категорий со всей структурой вложенных категорий и типов")
