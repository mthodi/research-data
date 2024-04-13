# ========================================================================
# Full pipeline to download, parse and store PCH data
# Combines download_pch_data.py and parse_pch_files.py
# ========================================================================

import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import config
from datetime import datetime
import gzip
import io
from tqdm import tqdm
import bgp_rib

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
        return extracted.decode()
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


def parse_rib_from_file(rib_dump, file_name):
    """Parses RIB dumps in-memory.
    @param rib_dump : the extracted content of the RIB dump
    @param file_name : the name of the file from the URL. Used to get the date
    @return: list of dicts or empty
    """
    rc_name = file_name.split("pch.net")[0] + "pch.net"

    buf = io.StringIO()
    buf.write(rib_dump)
    buf.seek(0)
    bgp_entries = []
    for entry_n, bgp_entry in enumerate(bgp_rib.BGPRIB.parse_cisco_show_ip_bgp_generator(buf)):
        network = bgp_entry[0]  # destination prefix
        next_hop = bgp_entry[2]  # NextHop
        as_path = bgp_entry[6]  # ASPath
        as_path_length = len(as_path)
        if as_path:
            nextas = int(as_path[0])
        else:
            nextas = '0'
        if as_path:
            try:
                originas = int(as_path[as_path_length - 1])
            except Exception as e:
                originas = as_path[as_path_length - 1].split('{')[1].split('}')[0].split(',')[0]
        else:
            originas = '0'
        # Origin
        if bgp_entry[7] == 'i':
            origin = "IGP"
        elif bgp_entry[7] == 'e':
            origin = "EGP"
        elif bgp_entry[7] == "?":
            origin = "INCOMPLETE"
        else:
            print(
                f"Found invalid prefix at bgp entry {entry_n}, with content {bgp_entry} in {file_name}")
            # ignore this line and continue
            continue
        full_as_path = " ".join(as_path)
        if len(next_hop) == 0:
            next_hop = 'NULL'
        if as_path_length == 0:
            full_as_path = 'NULL'
        else:
            full_as_path = " ".join(as_path)
        bgp_entries.append({
            'OriginAS': int(originas), 'NextAS': int(nextas),
            "Routecollector": rc_name,
            'NextHop': next_hop, 'Network': network, 'ASPath': full_as_path,
            'ASpathlength': as_path_length, 'Origin': origin,
        })
    df = pd.DataFrame(bgp_entries)
    df.to_parquet(output_dir / f"{rc_name}.parquet")
    return 1


def download_and_parse_rib_dump(url):
    rib_dump = download_rib_dump(url)
    if rib_dump:
        parse_rib_from_file(rib_dump, url)


# download rib dumps in parallel
def main():
    urls = make_urls()
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(download_and_parse_rib_dump, url) for url in tqdm(urls)}
        for future in tqdm(as_completed(futures), total=len(futures)):
            try:
                r = future.result()
            except Exception as e:
                print(e)


if __name__ == '__main__':
    main()
