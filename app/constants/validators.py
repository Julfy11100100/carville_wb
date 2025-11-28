# Допустимые поля для обновления с допустимыми типами
ALLOWED_FIELDS_FOR_UPDATES = {
    "title": str,
}

# Допустимые поля для сопоставления
# Тут может быть либо поле из VALID_WB_FIELDS либо id характеристики в которой будем искать значение поля
VALID_WB_FIELD_PATTERN = r'characteristics_\d+'
VALID_WB_FIELDS = ["vendorCode", "barcode"] # поля по котором сопоставляем на стороне WB
VALID_CARVILLE_FIELDS = ["code", "bar_code"] # поля по котором сопоставляем на стороне carville
VALID_COMPARISON_FIELDS = ["title",] # поля для сравнения