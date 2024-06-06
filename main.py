import math
import os
import time
from datetime import datetime, timedelta

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from dotenv import load_dotenv

TOPIC_PREFIX = 'i483-sensors-s2410064'
load_dotenv()

consumer = KafkaConsumer(bootstrap_servers=[os.getenv('BOOTSTRAP_SERVER')], group_id='bmp180_scd41')
producer = KafkaProducer(bootstrap_servers=[os.getenv('BOOTSTRAP_SERVER')])

topic_partitions = []
topic_partitions.append(TopicPartition(f'{TOPIC_PREFIX}-BMP180-temperature', 0))
topic_partitions.append(TopicPartition(f'{TOPIC_PREFIX}-SCD41-co2', 0))

consumer.assign(topic_partitions)
consumer.seek_to_end(topic_partitions[1])

count = 0
prev_co2 = None

while True:
    unix_timestamp_five_minutes_ago_s = datetime.timestamp(datetime.now() - timedelta(minutes=5))
    unix_timestamp_five_minutes_ago_ms = unix_timestamp_five_minutes_ago_s * 1000

    timestamps = {topic_partitions[0]: unix_timestamp_five_minutes_ago_ms}
    bmp180_temperature_start = consumer.offsets_for_times(timestamps)

    consumer.seek(topic_partitions[0], bmp180_temperature_start[topic_partitions[0]].offset)

    messages = consumer.poll(100)

    for topic_partition in messages.keys():
        if topic_partition.topic == f'{TOPIC_PREFIX}-BMP180-temperature':
            if count >= 30:
                temperatures_last_five_minutes = []

                for record in messages[topic_partition]:
                    temperature = float(record.value.decode())
                    temperatures_last_five_minutes.append(temperature)

                average_temperature_last_five_minutes = round(sum(temperatures_last_five_minutes) / len(temperatures_last_five_minutes), 1)
                producer.send(f'{TOPIC_PREFIX}-BMP180_avg-temperature', str(average_temperature_last_five_minutes).encode())

                count = 0
        elif topic_partition.topic == f'{TOPIC_PREFIX}-SCD41-co2':
            for record in messages[topic_partition]:
                current_co2 = int(record.value.decode())
                if (prev_co2 is not None) and (prev_co2 < 700 < current_co2 or current_co2 < 700 < prev_co2):
                    if current_co2 > 700:
                        producer.send(f'{TOPIC_PREFIX}-co2_threshold-crossed', 'yes'.encode())
                    else:
                        producer.send(f'{TOPIC_PREFIX}-co2_threshold-crossed', 'no'.encode())
                prev_co2 = current_co2

    consumer.commit()

    time.sleep(1)
    count += 1


