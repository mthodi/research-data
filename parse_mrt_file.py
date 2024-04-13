#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ================================================================================
# This file takes a single MRT file as input and parses it into a pandas DataFrame.
# The data is then saved as parquet file.
# ================================================================================
import argparse
import bgpkit
import pandas as pd


# functions
def flatten_list_to_string(_list):
    """Flatten list to string."""
    if _list is None or len(_list) == 0:
        return None
    return ' '.join([str(elem) for elem in _list])


def parse_mrt_file(file_path, output_dir):
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
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', dest='file_path', default=None, help='Input MRT file path.')
    parser.add_argument('-o', dest='output_dir', default=None, help='Output directory for parquet files.')
    args = parser.parse_args()

    if not args.file_path or not args.output_dir:
        parser.print_help()
        return

    parse_mrt_file(args.file_path, args.output_dir)


if __name__ == '__main__':
    main()
