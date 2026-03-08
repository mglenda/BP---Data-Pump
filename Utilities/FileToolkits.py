import json

def load_json(filename: str) -> list[dict]:
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)
    

def load_sql_file(filename: str) -> str:
    with open(filename, "r", encoding="utf-8") as f:
        return f.read()