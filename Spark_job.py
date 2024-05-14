import subprocess

class Benchmark:
    def __init__(self, queryNumber, fileFormat, partitions):
        self.queryNumber = str(queryNumber)
        self.fileFormat = str(fileFormat)
        self.partitions = str(partitions)

    def StartBenchmark(self, queue):
        print("Benchmark started!")
        p = subprocess.run(["spark-submit", "--master", "spark://spark-HP-EliteDesk-800-G2-TWR:7077", "--executor-memory", "10g", "--packages", "org.apache.spark:spark-avro_2.12:3.2.2", "--conf", "spark.sql.shuffle.partitions="+self.partitions,"--class", "main.scala.TpchQuery", "target/scala-2.12/spark-tpc-h-queries_2.12-1.0.jar", "" + self.queryNumber, "" + self.fileFormat, "" + self.partitions], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
        queue.put("finish")