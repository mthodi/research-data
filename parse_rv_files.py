import pandas as pd
import config
import bgpkit
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
import os

base_dir = config.BASE_DATA_DIR
rv_raw_dir = base_dir / 'routeviews/rv_raw/'
output_dir = base_dir / 'paths/latest/routeviews/'
assert base_dir.exists()
assert rv_raw_dir.exists()
assert output_dir.exists()


# functions
def get_output_file_name(url):
    """Generates output file name from URL."""
    parts = url.split('/')
    collector_name = parts[3].split('.')[1]
    date_parts = parts[-1].split('.')[1]
    date = f"{date_parts[4:6]}_{date_parts[6:8]}_{date_parts[:4]}"
    output = f"route-views.{collector_name}_{date}"
    return output


def flatten_list_to_string(_list):
    """Flatten list to string."""
    if _list is None or len(_list) == 0:
        return None
    return ' '.join([str(elem) for elem in _list])


def parse_mrt_file(file_path):
    """Parse MRT file and return a list of dictionaries."""
    file_path = str(file_path)
    output_file_name = file_path.split('/')[-1]
    parser = bgpkit.Parser(url=file_path)

    elements = []
    more_items = True
    while more_items:
        e = parser.parse_next()
        if e is None:
            more_items = False
        else:
            # check if prefix is ipv6
            if ':' in e['prefix']:
                break
            elements.append(e)
    _df = pd.DataFrame(elements)
    if _df.empty:
        return 0
    _df['origin_asns'] = _df['origin_asns'].apply(flatten_list_to_string)
    _df['communities'] = _df['communities'].apply(flatten_list_to_string)
    # pick only columns we need
    _df_db = _df[['peer_ip', 'peer_asn', 'prefix', 'as_path', 'origin_asns', 'origin', 'communities',
                  'atomic', 'aggr_asn', 'aggr_ip']]
    # save to db
    output_file_name = output_file_name.split('.bz2')[0]
    _df.to_parquet(f"{output_dir}/{output_file_name}.parquet")


def main():
    rv_raw_files = list(rv_raw_dir.glob('*'))
    print(f"Number of files: {len(rv_raw_files)}")

    batch_size = 2
    for i in range(0, len(rv_raw_files), batch_size):
        batch = rv_raw_files[i:i + batch_size]
        with ProcessPoolExecutor() as executor:
            futures = {executor.submit(parse_mrt_file, mrt_file) for mrt_file in tqdm(batch)}
            for future in tqdm(as_completed(futures), total=len(futures)):
                try:
                    r = future.result()
                except Exception as e:
                    print(f"An error occurred: {str(e)}")


if __name__ == '__main__':
    main()
