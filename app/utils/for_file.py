import json


def save_dict_to_json(data: dict, filename: str) -> None:
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def load_dict_from_json(filename: str) -> dict:
    with open(filename, 'r', encoding='utf-8') as f:
        return json.load(f)