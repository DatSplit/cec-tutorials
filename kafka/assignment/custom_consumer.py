import signal
import click 
import random
from confluent_kafka import Consumer
from fastavro import reader, parse_schema



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

def decode_avro_message(message):
    avro_file = io.BytesIO(message)
    avro_reader = reader(avro_file)
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
        print(decode_avro_message(msg.value()))

consume()