import asyncio
import csv
import aiohttp
from urllib.parse import urlparse
import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def is_valid_url(url):
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)


async def fetch_url(session, url):
    try:
        async with session.get(url) as response:
            status = response.status
            logger.info(f"({status}) {url}")
            return {"URL": url, "Status": status, "Error": ""}
    except Exception as e:
        logger.error(f"(ERROR) {url}")
        return {"URL": url, "Status": "ERROR", "Error": str(e)}


async def read_csv_and_process_urls(filename):
    invalid_urls = []
    results = []

    # Read CSV file
    with open(filename, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader, None)
        if not header:
            raise ValueError("CSV file is missing a header")
        urls = [row[0] for row in reader if row]

    # Validate URLs
    valid_urls = [url for url in urls if is_valid_url(url)]
    invalid_urls = [url for url in urls if not is_valid_url(url)]

    # Process valid URLs asynchronously with concurrency limit
    sem = asyncio.Semaphore(10)
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_url_with_limit(sem, session, url) for url in valid_urls]
        responses = await asyncio.gather(*tasks)
        results.extend(responses)

    return invalid_urls, results


async def fetch_url_with_limit(sem, session, url):
    async with sem:
        return await fetch_url(session, url)


def prepare_output_data(invalid_urls, results):
    return [
        {"URL": url, "Status": "Invalid",
            "Error": "Invalid URL format" if url else "Empty URL"}
        for url in invalid_urls
    ] + results


def write_to_csv(output_data, filename="url_statuses.csv"):
    fieldnames = ["URL", "Status", "Error"]
    with open(filename, "w", newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_data)


if __name__ == "__main__":
    import sys
    CSV_FILE = sys.argv[1] if len(sys.argv) > 1 else "Task_2_intern.csv"
    invalid_urls, results = asyncio.run(read_csv_and_process_urls(CSV_FILE))

    output_data = prepare_output_data(invalid_urls, results)
    success_count = sum(
        1 for item in output_data if str(item["Status"]) == "200")
    fail_count = len(output_data) - success_count

    write_to_csv(output_data)

    print(f"Processed {len(output_data)} URLs")
    print(f"Successes: {success_count}, Failures: {fail_count}")
