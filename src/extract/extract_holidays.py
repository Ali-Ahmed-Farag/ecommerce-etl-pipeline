import requests

API_URL = "https://brasilapi.com.br/api/feriados/v1/{year}"


def year_already_loaded(conn, load_year: int) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM raw.holidays WHERE load_year = %s LIMIT 1", (load_year,))
    return cur.fetchone() is not None


def extract_holidays_api(conn, load_year: int) -> list[dict] | None:
    if year_already_loaded(conn, load_year):
        return None
    resp = requests.get(API_URL.format(year=load_year), timeout=30)
    resp.raise_for_status()
    return resp.json()
