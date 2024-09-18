import signal
import click 
import random
import avro.io
import io
from confluent_kafka import Consumer
from avro.schema import parse
from io import BytesIO
from avro.io import DatumReader, BinaryDecoder
import avro.schema
import fastavro
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from fastavro import reader, parse_schema

experiment_started_schema = {
  "type": "record",
  "name": "experiment_started",
  "fields": [
    {"name": "experiment", "type": "string"},
    {"name": "timestamp", "type": "double"}
  ]
}


experiment_config_schema = parse("""{
    "type": "record", 
    "name": "ExperimentConfig", 
    "fields": [
        {
            "type": "string",
            "name": "experiment"
        },
        {
            "type": "string",
            "name": "researcher"
        },
        {
            "name": "sensors", 
            "type": {
                "type": "array",
                "items": "string"
            }
        }, 
        {
            "name": "temperature_range",
            "type": {
                "type": "record",
                "name": "temperature_range",
                "fields": [
                    {"name": "upper_threshold", "type": "float"},
                    {"name": "lower_threshold", "type": "float"}
                ]
            } 
        }
    ]
}""")

stabilization_started_schema = parse('''
{
    "type": "record",
    "name": "stabilization_started",
    "fields": [
        {"name": "experiment", "type": "string"},
        {"name": "timestamp", "type": "double"}
    ]
}
''')

sensor_temperature_measured_schema = parse('''
{
    "type": "record",
    "name": "sensor_temperature_measured",
    "fields": [
        {"name": "experiment", "type": "string"},
        {"name": "sensor", "type": "string"},
        {"name": "measurement_id", "type": "string"},
        {"name": "timestamp", "type": "double"},
        {"name": "temperature", "type": "float"},
        {"name": "measurement_hash", "type": "string"}
    ]
}
''')

experiment_terminated_schema = parse('''
{
    "type": "record",
    "name": "experiment_terminated",
    "fields": [
        {"name": "experiment", "type": "string"},
        {"name": "timestamp", "type": "double"}
    ]
}
''')


def signal_handler(sig, frame):
    print('EXITING SAFELY!')
    exit(0)

signal.signal(signal.SIGTERM, signal_handler)

c = Consumer({
    'bootstrap.servers': '13.60.146.188:19093,13.60.146.188:29093,13.60.146.188:39093',
    'group.id': f"{random.random()}",
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': 'true',
    'security.protocol': 'SSL',
    'ssl.ca.location': './auth/ca.crt',
    'ssl.keystore.location': './auth/kafka.keystore.pkcs12',
    'ssl.keystore.password': 'cc2023',
    'ssl.endpoint.identification.algorithm': 'none',
})
print("Consumer created")

def decode_avro_message(message, schema):
    # # Extract the schema
    # schema_start = message.index(b'{"type":"record"')
    # schema_end = message.index(b'}\x00', schema_start)
    # schema_json = message[schema_start:schema_end]
    # # Parse the schema
    # #schema = avro.schema.parse(schema_json)

    # # Create a DatumReader
    # reader = DatumReader(schema)

    # # Create a BinaryDecoder for the actual data
    # data_start = schema_end # Skip the sync marker
    # decoder = BinaryDecoder(io.BytesIO(message[data_start:]))
    # print(message[data_start:])
    # # Read the data
    # deserialized_data = reader.read(decoder)
    # Create a file-like object from the bytes
    avro_file = io.BytesIO(message)

    # Read the Avro data
    avro_reader = reader(avro_file)

    # Iterate through the records (there's usually just one in this case)
    for record in avro_reader:
        print(record)
        return record


@click.command()
@click.argument('topic')
def consume(topic: str): 
    c.subscribe(
        [topic], 
        on_assign=lambda _, p_list: print(p_list)
    )

    num_events = 0
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        num_events += 1
        if num_events % 1000 == 0:
            print(num_events)
        record_name = msg.headers()[0][1].decode('utf-8')
        print(record_name)
        print(msg.value())
        print(msg.headers())
        if record_name == 'sensor_temperature_measured':
            deserialized_msg = decode_avro_message(msg.value(), sensor_temperature_measured_schema)
        elif record_name == 'experiment_configured':
            deserialized_msg = decode_avro_message(msg.value(), experiment_config_schema)
        elif record_name == 'experiment_terminated':
            deserialized_msg = decode_avro_message(msg.value(), experiment_terminated_schema)
        elif record_name == 'experiment_started':
            deserialized_msg = decode_avro_message(msg.value(), experiment_started_schema)
        elif record_name == 'stabilization_started':
            deserialized_msg = decode_avro_message(msg.value(), stabilization_started_schema)
        print(deserialized_msg)

consume()