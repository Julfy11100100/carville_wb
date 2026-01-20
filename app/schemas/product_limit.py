from pydantic import Field, BaseModel


class LimitUsage(BaseModel):
    """Модель для usage и limit"""
    limit: int = Field(..., description="Максимальный лимит")


class ProductLimitResponse(BaseModel):
    """Ответ с лимитами товаров"""
    total: LimitUsage = Field(..., description="Общие лимиты товаров")
