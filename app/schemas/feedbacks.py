from typing import List, Optional

from pydantic import Field, model_validator, BaseModel


class FeedbackDoc(BaseModel):
    id: str
    text: Optional[str] = None
    pros: Optional[str] = None
    cons: Optional[str] = None
    product_valuation: Optional[int] = None
    created_date: Optional[str] = None
    product_name: Optional[str] = None
    vendor_code: Optional[str] = None
    brand_name: Optional[str] = None
    subject_id: Optional[int] = None
    bar_code: Optional[int] = None
    photos_amount: Optional[int] = None
    videos_amount: Optional[int] = None
    status: Optional[str] = None


class FeedbackListRequest(BaseModel):
    # Режим 1: относительный период (например 15h, 2d, 3w)
    period: str | None = Field(None, description="Период, например 15h, 2d, 3w")
    # Режим 2: абсолютный интервал дат/времени (UTC). Даты без времени трактуются как начало/конец дня
    date_from: str | None = Field(None,
                                  description="Начало интервала, ISO 8601 UTC, например 2025-11-01 или 2025-11-01T00:00:00Z")
    date_to: str | None = Field(None,
                                description="Конец интервала, ISO 8601 UTC, например 2025-11-30 или 2025-11-30T23:59:59Z")
    # Курсор для постраничной навигации (base64(JSON) со значениями search_after)
    cursor: str | None = Field(None, description="Курсор для продолжения постраничного получения (base64 от JSON)")


class FeedbackByValueRequest(BaseModel):
    vendor_code: Optional[str] = Field(None, description="vendor_code товара")
    barcode: Optional[int] = Field(None, description="Штрихкод товара")

    @model_validator(mode='after')
    def validate_exactly_one_parameter(self):
        """Проверяет, что передан ровно один из параметров: vendor_code или barcode"""
        has_vendor_code = bool(self.vendor_code and str(self.vendor_code).strip())
        has_barcode = bool(self.barcode and str(self.barcode).strip())

        if has_vendor_code and has_barcode:
            raise ValueError("Нельзя одновременно передавать vendor_code и barcode. Укажите только один из параметров.")

        if not has_vendor_code and not has_barcode:
            raise ValueError("Необходимо передать либо vendor_code, либо barcode.")

        return self


class FeedbackListResponse(BaseModel):
    feedbacks: List[FeedbackDoc] = Field(..., description="Список отзывов за период (по 1000 на страницу)")
    total: int = Field(..., description="Общее количество найденных отзывов (приблизительное)")
    has_next: bool = Field(..., description="Есть ли следующая страница данных в указанном интервале")
    next_cursor: Optional[str] = Field(None,
                                       description="Курсор для следующей страницы или null, если данных больше нет")


class FeedbackByValueResponse(BaseModel):
    feedbacks: List[FeedbackDoc] = Field(..., description="Список отзывов по значению (макс. 10000)")
    total: int = Field(..., description="Общее количество найденных отзывов")
    avg_rating: Optional[float] = Field(None, description="Средний рейтинг по отзывам или null, если нет рейтингов")
    ratings: dict = Field(default_factory=dict, description="Распределение рейтингов по количеству (ключи 1..5)")
