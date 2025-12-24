from typing import List, Optional

from pydantic import Field, model_validator, BaseModel


class ReviewDoc(BaseModel):
    id_review: str
    sku: Optional[int] = None
    text: Optional[str] = None
    published_at: Optional[str] = None
    rating: Optional[int] = None
    comments_amount: Optional[int] = None
    photos_amount: Optional[int] = None
    videos_amount: Optional[int] = None
    is_rating_participant: Optional[int] = None
    vendor_code: Optional[str] = Field(None, alias="offer_id", serialization_alias="vendor_code")  # в эластике хранится как offer_id
    product_name: Optional[str] = None
    barcodes: Optional[int] = None


class ReviewByValueRequest(BaseModel):
    vendor_code: Optional[str] = Field(None, description="vendor_code товара")
    barcode: Optional[int] = Field(None, description="Штрихкод товара")

    @model_validator(mode='after')
    def validate_exactly_one_parameter(self):
        """Проверяет, что передан ровно один из параметров: vendor_code или barcode"""
        has_vendor_code = self.vendor_code is not None and str(self.vendor_code).strip()
        has_barcode = self.barcode is not None

        if has_vendor_code and has_barcode:
            raise ValueError("Нельзя одновременно передавать vendor_code и barcode. Укажите только один из параметров.")

        if not has_vendor_code and not has_barcode:
            raise ValueError("Необходимо передать либо vendor_code, либо barcode.")

        return self


class ReviewByValueResponse(BaseModel):
    reviews: List[ReviewDoc] = Field(..., description="Список отзывов по значению (макс. 10000)")
    total: int = Field(..., description="Общее количество найденных отзывов")
    avg_rating: Optional[float] = Field(None, description="Средний рейтинг по отзывам или null, если нет рейтингов")
    ratings: dict = Field(default_factory=dict, description="Распределение рейтингов по количеству (ключи 1..5)")
