import os

INCOMING_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "incoming", "sales")


def extract_sales_csv(logical_date: str) -> dict:
    path = os.path.join(INCOMING_DIR, f"{logical_date}.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"missing incoming CSV: {path}")
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    return {"source_file": os.path.basename(path), "content": content, "logical_date": logical_date}
