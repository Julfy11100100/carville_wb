from pydantic import BaseModel, Field


class CheckTokenResponse(BaseModel):
    """Ответ проверки токена и разрешений"""
    status: str = Field(..., description="Статус проверки (success или error)")
    api_key_valid: bool = Field(..., description="Валиден ли API ключ")
    permissions_valid: bool = Field(..., description="Есть ли все необходимые разрешения")
