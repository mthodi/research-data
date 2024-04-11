import pandas as pd
import config
import bgpkit
import manage_storage
from tqdm import tqdm
import os

base_dir = config.base_dir
rv_raw_dir = base_dir / 'routeviews' / 'rv_raw/napafrica'
assert base_dir.exists()
assert rv_raw_dir.exists()
output_engine = config.make_engine('routes')
fm = manage_storage.FileManager(base_dir / 'routeviews')


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

    elements = parser.parse_all()
    rc_name = output_file_name.split('_')[0]
    _df = pd.DataFrame(elements)
    if _df.empty:
        return 0
    _df['origin_asns'] = _df['origin_asns'].apply(flatten_list_to_string)
    _df['communities'] = _df['communities'].apply(flatten_list_to_string)
    _df['route_collector_name'] = rc_name
    _df['timestamp'] = pd.to_datetime(_df['timestamp'], unit='s')
    # pick only columns we need
    _df_db = _df[['timestamp', 'peer_ip', 'peer_asn', 'prefix', 'as_path', 'origin_asns', 'origin', 'communities',
                  'route_collector_name', ]]
    # save to db
    tt = output_file_name.split('.bz2')[0]
    _date = tt.split('_')[3] + "-" + tt.split('_')[1] + "-" + tt.split('_')[2]
    output_dir = fm.get_directory_name(_date)
    _df.to_parquet(f"{output_dir}/{output_file_name}.parquet")
    # delete file
    os.remove(file_path)


def main():
    rv_raw_files = list(rv_raw_dir.glob('*'))
    print(f"Number of files: {len(rv_raw_files)}")
    for mrt_file in tqdm(rv_raw_files[10:20]):
        try:
            print(f">>> Parsing {mrt_file}")
            parse_mrt_file(mrt_file)
        except Exception as e:
            print(f"An error occurred: {str(e)}")


if __name__ == '__main__':
    main()
