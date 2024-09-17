import signal
import click 
import random
import avro.io
import io
from confluent_kafka import Consumer
from avro.schema import parse
experiment_started_schema = {
  "type": "record",
  "name": "experiment_started",
  "fields": [
    {"name": "experiment", "type": "string"},
    {"name": "timestamp", "type": "double"}
  ]
}


experiment_config_schema = {
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
}

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

def avro_deserializer(schema, data):
    if not data:
        return None
    bytes_reader = io.BytesIO(data)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    return reader.read(decoder)

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
        if record_name == 'sensor_temperature_measured':
            deserialized_msg = avro_deserializer(sensor_temperature_measured_schema, msg.value())
        elif record_name == 'experiment_configured':
            deserialized_msg = avro_deserializer(experiment_config_schema, msg.value())
        elif record_name == 'experiment_terminated':
            deserialized_msg = avro_deserializer(experiment_terminated_schema, msg.value())
        elif record_name == 'experiment_started':
            deserialized_msg = avro_deserializer(experiment_started_schema, msg.value())
        elif record_name == 'stabilization_started':
            deserialized_msg = avro_deserializer(stabilization_started_schema, msg.value())
        print(deserialized_msg)

consume()