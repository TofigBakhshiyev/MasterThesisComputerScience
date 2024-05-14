import csv
from csv import reader


column_names = {
    "nation": ["n_nationkey", "n_name", "n_regionkey", "n_comment"],
    "region": ["r_regionkey", "r_name", "r_comment"],
    "part": ["p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment"],
    "supplier": ["s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"],
    "partsupp": ["ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"],
    "customer": ["c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"],
    "orders": ["o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"],
    "lineitem": ["l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"]
}

def convert(filename):
    file = open("./csv/"+filename+".csv", 'a+', newline ='')

    with file:   
        with open("./dbgen/"+filename+".tbl") as f:    
                csv_reader = reader(f)
                header = csv_reader
                if header != None:
                        write = csv.writer(file)
                        write.writerow(column_names[filename])
                        # Iterate over each row after the header in the csv
                        for row in csv_reader:
                            # row variable is a list that represents a row in csv
                            row = "".join(row).split("|")
                            #print(row)
                            write.writerow(row)
                
            
 
names = ["region", "customer", "lineitem", "nation", "orders", "part", "partsupp", "supplier"]

for i in names:
    convert(i) 
    
print("finished")