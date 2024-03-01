Written using Python 3.12

Sieve index Prototype to index parquet files. 

How to run example:
1. Generate TPC-H data set (tpc-h/tpch-tools/README.md):
    1. ./bin/build-tpch-dbgen.sh
    2. ./bin/gen-tpch-data.sh -s 1000
2. pip install -r req.txt
3. Run table_init.py (generates parquet dataset)
4. Run main.py
