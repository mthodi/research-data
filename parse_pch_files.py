# =======================================================================
# Parse and save RIB dumps from PCH.
# =======================================================================
import pandas as pd

import bgp_rib
import gzip
import io
import config
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

input_dir = config.BASE_DATA_DIR / 'pch/raw/'
output_dir = config.BASE_DATA_DIR / 'paths/latest/pch/'


def parse_rib_from_file(file_name):
    """Parses RIB dumps in-memory.
    @param file_name : the name of the file from the URL. Used to get the date
    @return: list of dicts or empty
    """
    rc_name = file_name.stem.split("pch.net")[0] + "pch.net"
    with open(file_name, 'rb') as f:
        rib_dump = gzip.decompress(f.read())
    buf = io.StringIO()
    buf.write(rib_dump.decode())
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


def main():
    files = list(input_dir.glob('*'))
    print(f"Number of files: {len(files)}")
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = {executor.submit(parse_rib_from_file, file) for file in tqdm(files)}
        for future in tqdm(as_completed(futures), total=len(futures)):
            try:
                r = future.result()
            except Exception as e:
                print(e)


if __name__ == '__main__':
    main()
