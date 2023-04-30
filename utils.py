import json


def read_json(file) -> list:
    """Читает json-файл"""
    with open(file, 'r', encoding="utf8") as f:
        data = json.load(f)
    return data



if __name__ == '__main__':
    employers = read_json('employers.json')
    isinstance(employers, list)
    get_employers(employers)