#!/usr/bin/env python
# ========================================================================================================
# Downloads the latest PCH RIB dumps and saves to disk.
# ========================================================================================================
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import config
from datetime import datetime
from tqdm import tqdm

output_dir = config.BASE_DATA_DIR / 'pch/raw/'
output_dir.mkdir(parents=True, exist_ok=True)


def download_rib_dump(url):
    """Downloads a rib dump from a given PCH URL """
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:103.0) Gecko/20100101 Firefox/103.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
    }
    file_name = url.split("/")[-1]
    try:
        extracted = b""
        with requests.get(url, stream=True, headers=headers) as res:
            extracted = res.content
            with open(output_dir / file_name, "wb") as f:
                f.write(extracted)
    except Exception as e:
        print(e)


def make_urls():
    engine = config.make_engine("arda")
    df = pd.read_sql_query("SELECT name FROM api_routecollector", engine)
    route_collectors = df["name"].tolist()
    base_url = "https://www.pch.net/resources/Routing_Data/IPv4_daily_snapshots"
    # get today's date in YYYY-MM-DD format
    today = datetime.today().strftime("%Y-%m-%d")
    # if the time is before 17hrs UTC, use yesterday's date (data is updated at 17hrs UTC)
    if datetime.now().hour < 17:
        today = datetime.today() - pd.Timedelta(days=1)
        today = today.strftime("%Y-%m-%d")
    year, month, day = today.split("-")
    urls = [f"{base_url}/{year}/{month.zfill(2)}/{rc}/{rc}-ipv4_bgp_routes.{year}.{month.zfill(2)}.{day.zfill(2)}.gz"
            for rc in route_collectors]
    print(f">>> Generated {len(urls)} URLs for download.")
    return urls


# download rib dumps in parallel
def main():
    urls = make_urls()
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(download_rib_dump, url) for url in tqdm(urls)}
        for future in tqdm(as_completed(futures), total=len(futures)):
            try:
                r = future.result()
            except Exception as e:
                print(e)


if __name__ == '__main__':
    main()
