## Master Thesis: Hard or/and Soft Tuning of Spark Ecosystem Toward Query Energy Efficiency on Different File Formats

<p>
This thesis explores the energy efficiency of executing TPCH queries within the Apache Spark framework, explicitly focusing on diverse file formats (Parquet, CSV, Avro, and TBL) and varying partition sizes in a standalone configuration. The assessment measures energy consumption during the data reading and query processing phases. Initial comparisons are made regarding the characteristics of Parquet, CSV, and Avro formats, analysing their impact on the query performance of Spark. Additionally, the study investigates Spark's standalone configuration, scrutinising cluster settings, resource allocation, and hardware optimizations that influence energy usage during query execution. An integral part of this exploration involves comprehending how different partition sizes influence energy consumption. The evaluation systematically assesses the impact of partition sizes on IO operations, data shuffling, and overall energy consumption during query processing. Utilising TPCH queries as benchmarks, experiments are conducted across various file formats, partition sizes, and configurations. The outcomes offer practical insights for enhancing energy efficiency in Spark-based big data processing. This research contributes to the broader discourse on sustainable data processing, guiding practitioners to make energy-conscious decisions in Apache Spark environments.
</p>

# 

### Installation & Configuration
- Linux Ubuntu 64 bit 22.04.2 LTS operation system had been used
- Begin by reviewing the original TPCH Readme file to understand the instructions provided. Follow the guidelines outlined there to configure Apache Spark and execute any required commands specified in the document. [TPCH Readme file](README_TPCH.md)
- If you have a Yocto-Watt device, please refer to the instructions provided in the following link and proceed with the configuration. [Yocto-Watt Documentation](https://www.yoctopuce.com/EN/products/yocto-watt/doc/YWATTMK1.usermanual.html)
- Use converter python scripts and convert TBL table files to CSV, Avro and Parquet

#

### Run
- Help: `python3 benchmark.py --help` 
- if you want to run one query, just use this command: `python3 benchmark.py -p 78 -q 18 -f parquet`, it will run 78 partitions and 18th query.
- if you want to run all queries from 1-22, run this bash file `./automation.sh partition_number file_format`

#

### For further questions:
Personal contact: [tofiqbakhshiyev@gmail.com](mailto:tofiqbakhshiyev@gmail.com) <br>
University contact: [tofig.bakhshiyev@ut.ee](mailto:tofig.bakhshiyev@ut.ee) <br>
Supervisor contact: Simon Pierre Dembele [simon.pierre.dembele@ut.ee](mailto:simon.pierre.dembele@ut.ee)