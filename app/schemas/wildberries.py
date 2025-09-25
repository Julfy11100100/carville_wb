from typing import Optional, Dict, Union, List, Any

from pydantic import BaseModel, Field


class ProductFilters(BaseModel):
    """Фильтры для получения списка товаров"""
    search: Optional[str] = Field(None, description="Поиск по названию товара")
    limit: int = Field(100, ge=1, le=1000, description="Количество товаров")
    offset: int = Field(0, ge=0, description="Смещение для пагинации")


class ProductCreateItem(BaseModel):
    """Модель для создания товара"""
    subject_id: int = Field(..., description="ID предмета")
    vendor_code: str = Field(..., max_length=75, description="Артикул продавца")
    title: str = Field(..., max_length=60, description="Наименование товара")
    description: str = Field(..., description="Описание товара")
    brand: str = Field(..., max_length=50, description="Бренд")
    dimensions: Dict[str, Union[int, float]] = Field(..., description="Габариты и вес")
    characteristics: List[Dict[str, Any]] = Field(..., description="Характеристики товара")
    sizes: List[Dict[str, Any]] = Field(..., description="Размеры товара")


class ProductUpdate(BaseModel):
    """Модель для обновления товара"""
    vendor_code: str = Field(..., description="Артикул продавца")
    title: Optional[str] = Field(None, max_length=60, description="Наименование товара")
    description: Optional[str] = Field(None, description="Описание товара")
    brand: Optional[str] = Field(None, max_length=50, description="Бренд")
    dimensions: Optional[Dict[str, Union[int, float]]] = Field(None, description="Габариты и вес")
    characteristics: Optional[List[Dict[str, Any]]] = Field(None, description="Характеристики")
    sizes: Optional[List[Dict[str, Any]]] = Field(None, description="Размеры")


class RateLimitInfo(BaseModel):
    """Информация о лимитах API"""
    remaining: Optional[int] = Field(None, description="Оставшееся количество запросов")
    limit: Optional[int] = Field(None, description="Общий лимит запросов")
    reset: Optional[int] = Field(None, description="Время сброса лимита в секундах")
    retry_after: Optional[int] = Field(None, description="Время ожидания при 429")
