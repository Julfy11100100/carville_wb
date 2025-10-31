from fastapi import HTTPException, status

from app.schemas.product_match import ProductMatchRequest

def validate_product_match_request(request: ProductMatchRequest) -> ProductMatchRequest:
    """Dependency для валидации запроса"""
    ProductMatchValidator.validate_match_request(request)
    return request

class ProductMatchValidator:
    """Валидатор для запросов на сопоставление товаров"""

    @staticmethod
    def validate_match_request(request: ProductMatchRequest):
        """Валидация запроса на сопоставление товаров"""

        # Валидация полей сопоставления
        valid_wb_fields = ["vendorCode",]  # то что разрешаем выбирать на wb
        if request.wb_match_field not in valid_wb_fields:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "Неверное поле wb_match_field",
                    "details": f"wb_match_field должен быть одним из: {', '.join(valid_wb_fields)}"
                }
            )

        valid_carville_fields = ["code"]  # то что разрешаем выбирать у нас
        if request.carville_match_field not in valid_carville_fields:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "Неверное поле carville_match_field",
                    "details": f"carville_match_field должен быть одним из: {', '.join(valid_carville_fields)}"
                }
            )

        valid_comparison_fields = ["subjectName"]  # поля которые будем сравнивать на рекомендуемое
        if request.comparison_field not in valid_comparison_fields:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "Неверное поле comparison_field",
                    "details": f"comparison_field должен быть одним из: {', '.join(valid_comparison_fields)}"
                }
            )

        # Валидация category_id
        if request.category_id <= 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "Неверное category_id",
                    "details": "category_id должен быть"
                }
            )
