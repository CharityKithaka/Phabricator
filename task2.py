import csv
import requests
from requests.exceptions import RequestException
import time

CSV_FILENAME = "Task 2 - Intern.csv"
TIMEOUT = (5, 10)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    )
}
# Track statistics summaries 
success_count = 0
fail_count = 0

# set up a session
with requests.Session() as session:
    session.headers.update(HEADERS)
    with open(CSV_FILENAME, newline='', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            url = row.get("url") or row.get("URL") or next(
                iter(row.values()), "").strip()
            if not url or not url.startswith(("http://", "https://")):
                print(f"(Invalid) {url}" if url else "(Empty URL)")
                fail_count += 1
                continue
            try:
                response = session.head(
                    url, timeout=TIMEOUT, allow_redirects=True)
                if response.status_code == 405:
                    response = session.get(
                        url, timeout=TIMEOUT, allow_redirects=True)
                print(f"({response.status_code}) {url}")
                if response.status_code == 200:
                    success_count += 1
                else:
                    fail_count += 1
            except RequestException as e:
                error_type = type(e).__name__.replace("Exception", "")
                print(f"({error_type}) {url} - {str(e).split(':')[0]}")
                fail_count += 1
            time.sleep(0.5)
