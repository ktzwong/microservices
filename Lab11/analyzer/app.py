import json
import connexion
import yaml
import logging
import logging.config
import random
import time
from pykafka import KafkaClient
from pykafka.exceptions import KafkaException
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware

with open('/config/analyzer_log_config.yml', 'r') as f:
    LOG_CONFIG = yaml.safe_load(f.read())

logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger('basicLogger')

with open('/config/analyzer_config.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())
    
class KafkaClientWrapper:
    def __init__(self, hostname):
        self.hostname = hostname
        self.client   = None
        self._connect()

    def _connect(self):
        """Infinite retry until client is created."""
        while True:
            try:
                self.client = KafkaClient(hosts=self.hostname)
                logger.info("Analyzer Kafka client created.")
                return
            except KafkaException as e:
                logger.warning(f"Failed to create Kafka client: {e}")
                self.client = None
                time.sleep(random.randint(500, 1500) / 1000)

    def get_topic_consumer(self, topic_name):
        """
        Returns a fresh consumer that reads from the beginning of the topic.
        If the client is stale/broken, reconnects first.
        """
        while True:
            try:
                topic    = self.client.topics[topic_name]
                consumer = topic.get_simple_consumer(
                    reset_offset_on_start=True,
                    consumer_timeout_ms=1000
                )
                return consumer
            except KafkaException as e:
                logger.warning(f"Failed to get consumer, reconnecting: {e}")
                self.client = None
                self._connect()


HOSTNAME       = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
kafka_client   = KafkaClientWrapper(HOSTNAME)
TOPIC_NAME     = str.encode(app_config['events']['topic'])


def get_match_history(index):
    logger.info(f"Get request for match_history at index {index}")

    consumer = kafka_client.get_topic_consumer(TOPIC_NAME)
    counter  = 0

    for msg in consumer:
        data = json.loads(msg.value.decode('utf-8'))
        if data.get('type') == 'match_history':
            if counter == index:
                logger.info(f"Found match_history at index {index}")
                return data.get('payload', {}), 200
            counter += 1

    logger.info(f"No match_history event at index {index}")
    return {"message": f"No match_history event at index {index}!"}, 404


def get_champion_winrate(index):
    logger.info(f"Get request for champion_winrate at index {index}")

    consumer = kafka_client.get_topic_consumer(TOPIC_NAME)
    counter  = 0

    for msg in consumer:
        data = json.loads(msg.value.decode('utf-8'))
        if data.get('type') == 'champion_winrate':
            if counter == index:
                logger.info(f"Found champion_winrate at index {index}")
                return data.get('payload', {}), 200
            counter += 1

    logger.info(f"No champion_winrate event at index {index}")
    return {"message": f"No champion_winrate event at index {index}!"}, 404


def get_reading_stats():
    logger.info("Get request for stats")

    consumer             = kafka_client.get_topic_consumer(TOPIC_NAME)
    num_match_history    = 0
    num_champion_winrate = 0

    for msg in consumer:
        data = json.loads(msg.value.decode('utf-8'))
        if data.get('type') == 'match_history':
            num_match_history += 1
        elif data.get('type') == 'champion_winrate':
            num_champion_winrate += 1

    stats = {
        "num_match_history":    num_match_history,
        "num_champion_winrate": num_champion_winrate
    }

    logger.info(f"Returning stats: {stats}")
    return stats, 200


app = connexion.FlaskApp(__name__, specification_dir='')

app.add_middleware(
    CORSMiddleware,
    position=MiddlewarePosition.BEFORE_EXCEPTION,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_api(
    'openapi.yaml',
    strict_validation=True,
    validate_responses=True
)

if __name__ == '__main__':
    app.run(port=8025, host="0.0.0.0")
