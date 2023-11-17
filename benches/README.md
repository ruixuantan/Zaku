# Benchmarks

This directory contains the TPCH benchmark for query 1.

## Setting up
1. Download the TPCH benchmark tool [here](https://www.tpc.org/tpc_documents_current_versions/download_programs/tools-download-request5.asp?bm_type=TPC-H&bm_vers=3.0.1&mode=CURRENT-ONLY).
1. Generate the csv file required, `lineitem.csv` and place it in this directory. [More instructions](https://gist.github.com/yunpengn/6220ffc1b69cee5c861d93754e759d08) on how to use the TPCH tool.
1. Run the following command to insert the schema into the csv file:
```sh
sed -i raw '1s/^/l_orderkey|l_partkey|l_suppkey|l_linenumber|l_quantity|l_extendedprice|l_discount|l_tax|l_returnflag|l_linestatus|l_shipdate|l_commitdate|l_receiptdate|l_shipinstruct|l_shipmode|l_comment\n/' lineitem.csv && rm lineitem.csvraw
```
