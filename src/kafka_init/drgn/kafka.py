import json
from confluent_kafka import Producer, Consumer, TopicPartition, KafkaError

from drgn.config import env_config
from typing import cast, Generator

kafka_config = {
    "bootstrap.servers": env_config["kafka"]["bootstrap_servers"],
    "security.protocol": env_config["kafka"]["security_protocol"],
}


class KafkaClient:
    def __init__(self, group_id: str = __file__):
        self._consumer_config = kafka_config | {
            "group.id": group_id,
            "on_commit": lambda err, topics: print(err, topics),
            "enable.partition.eof": True,
        }
        self._producer_config = kafka_config
        self._consumer = None
        self._producer = None

    @property
    def consumer(self) -> Consumer:
        if not self._consumer:  # lazy
            self._consumer = Consumer(cast(dict, self._consumer_config))
        return self._consumer

    @property
    def producer(self) -> Producer:
        if not self._producer:  # lazy
            self._producer = Producer(cast(dict, self._producer_config))
        return self._producer

    def _get_topic_partition(self, topic: str):
        return TopicPartition(topic, 0, 0)

    def consume(self, topic: str) -> Generator[list[dict], None, None]:
        self._consumer_config["auto.offset.reset"] = env_config["consumer"][
            "offset_reset"
        ]
        self._consumer_config["enable.auto.commit"] = env_config["consumer"][
            "enable_auto_commit"
        ]
        self.consumer.assign([self._get_topic_partition(topic)])
        consume_size = int(env_config["consumer"]["consume_size"])
        consume_timeout = float(env_config["consumer"]["consume_timeout"])

        messages = []
        while True:
            msgs = self.consumer.consume(consume_size, timeout=consume_timeout)
            if not msgs:
                continue

            messages = []
            for msg in msgs:
                if error := msg.error():
                    if error.code() == KafkaError._PARTITION_EOF:  # type: ignore
                        continue
                    else:
                        print(f"ERROR - {type(KafkaError)} - {error}")
                elif value := msg.value():
                    messages.append(json.loads(value.decode("utf-8")))

                yield messages
                self.consumer.commit()

    def produce(self, topic: str, message: bytes):
        self.producer.produce(topic, message)
        self.producer.flush()
