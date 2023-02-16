import avro.schema

from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader


def save_records(records: list, schema_file: str, avro_file: str):
    schema = avro.schema.parse(open(schema_file, "rb").read())
    with DataFileWriter(open(avro_file, "wb"), DatumWriter(), \
        schema) as writer:
        [writer.append(record) for record in records]
            

def read_records(avro_file: str) -> list:
    records = []
    with DataFileReader(open(avro_file, "rb"), DatumReader()) as reader:
        [records.append(record) for record in reader]
    return records


def back_pressure_save_records(records: iter, schema_file: str, \
    avro_file: str):
    schema = avro.schema.parse(open(schema_file, "rb").read())
    with DataFileWriter(open(avro_file, "wb"), DatumWriter(), \
        schema) as writer:
        while True:
            record = next(records, False)
            if record:
                writer.append(record)
            else: 
                break


def back_pressure_read_records(avro_file: str) -> iter:
    with DataFileReader(open(avro_file, "rb"), DatumReader()) as reader:
        for record in reader:
            yield record
