import csv
import re
import time
import os
from typing import List, Tuple, Optional
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://en.lottolyzer.com/history/vietnam/mega-645/page/{page}/per-page/50/summary-view"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
}

ROW_REGEX = re.compile(
    r"^\s*(\d{3,4})\s+(\d{4}-\d{2}-\d{2})\s+([0-9]{1,2},[0-9]{1,2},[0-9]{1,2},[0-9]{1,2},[0-9]{1,2},[0-9]{1,2})\b"
)

def fetch_page_html(page: int, timeout: int = 20) -> Optional[str]:
    url = BASE_URL.format(page=page)
    for _ in range(3):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
            if resp.status_code == 200:
                return resp.text
            time.sleep(1.5)
        except requests.RequestException:
            time.sleep(1.5)
    return None

def parse_rows_from_html(html: str) -> List[Tuple[str, List[int]]]:
    soup = BeautifulSoup(html, "html.parser")
    results: List[Tuple[str, List[int]]] = []

    tables = soup.find_all("table")
    for tb in tables:
        thead = tb.find("thead")
        if thead and "Winning No" not in thead.get_text(" ", strip=True):
            continue
        tbody = tb.find("tbody") or tb
        for tr in tbody.find_all("tr"):
            txt = tr.get_text(" ", strip=True)
            m = ROW_REGEX.search(txt)
            if m:
                date_str = m.group(2)
                nums = list(map(int, m.group(3).split(",")))
                if len(nums) == 6 and all(1 <= n <= 45 for n in nums):
                    results.append((date_str, nums))
    if results:
        return results

    # fallback nếu không parse được từ table
    text = soup.get_text("\n", strip=True)
    for line in text.splitlines():
        m = ROW_REGEX.search(line)
        if m:
            date_str = m.group(2)
            nums = list(map(int, m.group(3).split(",")))
            if len(nums) == 6 and all(1 <= n <= 45 for n in nums):
                results.append((date_str, nums))
    return results

def crawl_all(max_pages: int = 60, sleep_sec: float = 1.2) -> List[Tuple[str, List[int]]]:
    all_rows: List[Tuple[str, List[int]]] = []
    for page in range(1, max_pages + 1):
        html = fetch_page_html(page)
        if not html:
            break
        rows = parse_rows_from_html(html)
        if not rows:
            break
        all_rows.extend(rows)
        time.sleep(sleep_sec)

    seen = set()
    unique_rows: List[Tuple[str, List[int]]] = []
    for date_str, nums in all_rows:
        key = (date_str, tuple(nums))
        if key not in seen:
            seen.add(key)
            unique_rows.append((date_str, nums))

    unique_rows.sort(key=lambda x: x[0])
    return unique_rows

def write_csv(rows: List[Tuple[str, List[int]]], out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Date", "Num1", "Num2", "Num3", "Num4", "Num5", "Num6"])
        for date_str, nums in rows:
            w.writerow([date_str] + nums)

def crawl_and_save_csv(path: str = "static/mega645.csv"):
    rows = crawl_all()
    write_csv(rows, path)
    return path

if __name__ == "__main__":
    output_file = "static/mega645.csv"
    crawl_and_save_csv(output_file)
    print(f"[DONE] Saved {output_file}")
