import csv
import io
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from fastavro import parse_schema


def convert(filename):
    # Define Avro schema
    schema = avro.schema.parse(open(f"./avro/schemas/{filename}.avsc", "rb").read())

    # Open CSV file for reading
    with open(f"./csv/{filename}.csv", "r") as csvfile:

        # Open Avro file for writing
        with open(f"./avro/{filename}.avro", "wb") as avrofile:
            writer = DataFileWriter(avrofile, DatumWriter(), schema)

            # Read CSV file line by line and write each line to Avro file
            reader = csv.DictReader(csvfile)      
            for row in reader:
                writer.append(row)

            writer.close()

names = ["nation", "orders", "part", "partsupp", "supplier", "lineitem"]

for name in names:
    convert(name) 
 
print("Done")