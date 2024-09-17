import signal
import click
import random
import fastavro
from confluent_kafka import Consumer
from io import BytesIO

def signal_handler(sig, frame):
    print('EXITING SAFELY!')
    exit(0)

signal.signal(signal.SIGTERM, signal_handler)

# AVRO schemas
schemas = {
    "sensor_temperature_measured": {
        "type": "record",
        "name": "SensorTemperatureMeasured",
        "fields": [
            {"name": "experiment", "type": "string"},
            {"name": "sensor", "type": "string"},
            {"name": "measurement_id", "type": "string"},
            {"name": "timestamp", "type": "float"},
            {"name": "temperature", "type": "float"},
            {"name": "measurement_hash", "type": "string"}
        ]
    },
    "experiment_terminated": {
        "type": "record",
        "name": "ExperimentTerminated",
        "fields": [
            {"name": "experiment", "type": "string"},
            {"name": "timestamp", "type": "float"}
        ]
    }
}

def deserialize_avro(message_value: bytes, schema: dict) -> dict:
    """ Deserialize Avro message to a dictionary. """
    reader = fastavro.reader(BytesIO(message_value), reader_options={'schema': schema})
    for record in reader:
        return record
    return {}

def create_consumer():
    return Consumer({
        'bootstrap.servers': '13.60.146.188:19093',
        'group.id': f"{random.random()}",
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': 'true',
        'security.protocol': 'SSL',
        'ssl.ca.location': './auth/ca.crt',
        'ssl.keystore.location': './auth/kafka.keystore.pkcs12',
        'ssl.keystore.password': 'cc2023',
        'ssl.endpoint.identification.algorithm': 'none',
    })

@click.command()
@click.argument('topic')
def consume(topic: str): 
    c = create_consumer()
    c.subscribe([topic], on_assign=lambda _, p_list: print(p_list))

    num_events = 0
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

 
        record_name = msg.headers().get('record_name', None).decode('utf-8') if msg.headers() else None
        if record_name not in schemas:
            print(f"Unknown record type: {record_name}")
            continue


        deserialized_message = deserialize_avro(msg.value(), schemas[record_name])
        print(record_name)
        print(deserialized_message)

        num_events += 1
        if num_events % 1000 == 0:
            print(num_events)

if __name__ == '__main__':
    consume()
