import uuid

import pandas as pd
import logging

logging.basicConfig(level = logging.DEBUG, format = '%(asctime)s [%(levelname)s] %(message)s')

def process_csv(csv_filename):
    logging.info(f'Processing {csv_filename}')
    count = 0
    for df in pd.read_csv(csv_filename, sep='|', header=None, chunksize=1000000):
        new_parquet_filename = f'./table/{uuid.uuid4()}.parquet'
        df.to_parquet(new_parquet_filename)
        count += 1
        logging.info(f'Written {count} of {csv_filename}: {new_parquet_filename}')

def main():
    for i in range(1, 11):
        process_csv(f'./tpc-h/tpch-tools/bin/tpch-data/lineitem.tbl.{i}')

if __name__ == '__main__':
    main()
