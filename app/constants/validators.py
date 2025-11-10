# Допустимые поля для обновления с допустимыми типами
ALLOWED_FIELDS_FOR_UPDATES = {
    "title": str,
}

# Допустимые поля для matchinga

VALID_WB_FIELDS = ["vendorCode",] # поля по котором сопоставляем на стороне WB
VALID_CARVILLE_FIELDS = ["code",] # поля по котором сопоставляем на стороне carville
VALID_COMPARISON_FIELDS = ["title",] # поля для сравнения