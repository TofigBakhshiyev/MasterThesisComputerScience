from multiprocessing import Process
from multiprocessing import Queue
from Yocto_job import Yocto
from Spark_job import Benchmark
import time
import argparse

processes = []

def main(query, fileformat, partitions):
    print("Program started")
    yocto = Yocto("any", query=query, fileformat=fileformat, partitions=partitions)
    benchmark = Benchmark(query, fileformat, partitions) 
    queue = Queue()
    queue.put("start")
    processes2 = Process(target = yocto.YoctoConnect, args=(queue,))
    processes1 = Process(target = benchmark.StartBenchmark, args=(queue, ))
    processes.append(processes1)
    processes.append(processes2)

    for p in processes:
        p.start()

    for p in processes:
        p.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Energy Benchmark for Spark applications with different file formats and query and conigurations.")
    parser.add_argument("-p", "--partitions", default=100)
    parser.add_argument("-q", "--query", default=None, nargs='?')
    parser.add_argument("-f", "--fileformat", default="parquet")
    args = parser.parse_args()
    partitions = (args.partitions)
    query = (args.query)
    fileformat = str(args.fileformat)
    fileformats = ["tbl", "csv", "parquet", "avro"]
    try: 
        print(f"Benchmark starts with file format: {fileformat} and query: {query} with {partitions} partitions.")
        start_time = time.time()
        main(str(query), str(fileformat), str(partitions))
        end_time = time.time()
        print(f"Finished: {end_time-start_time}") 
    except TypeError as e:
        print("Error: ", e)