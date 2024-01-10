import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092',  security_protocol="PLAINTEXT")

for i in range(10000):  # Produce 10 messages
    key = "key{}".format(i)
    value = random.randint(0, 1000)
    record = {
        'key': key,
        'value': value,
        'timestamp': int(time.time() * 1000)
    }

    print(json.dumps(record))
    future = producer.send(
        topic="sensors",
        value=json.dumps(record).encode("utf-8")
    )

    try:
        record_metadata = future.get(timeout=10)
        print(record_metadata)
    except Exception as e:
        print(f"Failed to send record: {e}")

#     time.sleep(random.randint(500, 2000) / 1000.0)

producer.flush()