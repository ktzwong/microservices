import connexion
from connexion import NoContent
import json
import datetime
import random
import time
from pykafka import KafkaClient
from pykafka.exceptions import KafkaException
import yaml
import logging
import logging.config
import uuid
import os
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware

with open('/config/receiver_log_config.yml', 'r') as f:
    LOG_CONFIG = yaml.safe_load(f.read())

logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger('basicLogger')

with open('/config/receiver_config.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

class KafkaProducerWrapper:
    def __init__(self, hostname, topic):
        self.hostname = hostname
        self.topic    = topic
        self.client   = None
        self.producer = None
        self.connect()

    def connect(self):
        while True:
            logger.debug("Trying to connect to Kafka...")
            if self._make_client():
                if self._make_producer():
                    logger.info("Kafka producer ready.")
                    break
            time.sleep(random.randint(500, 1500) / 1000)

    def _make_client(self):
        if self.client is not None:
            return True
        try:
            self.client = KafkaClient(hosts=self.hostname)
            logger.info("Kafka client created.")
            return True
        except KafkaException as e:
            logger.warning(f"Failed to create Kafka client: {e}")
            self.client   = None
            self.producer = None
            return False

    def _make_producer(self):
        if self.producer is not None:
            return True
        if self.client is None:
            return False
        try:
            topic         = self.client.topics[self.topic]
            self.producer = topic.get_sync_producer()
            return True
        except KafkaException as e:
            logger.warning(f"Failed to create producer: {e}")
            self.client   = None
            self.producer = None
            return False

    def produce(self, message_bytes):
        while True:
            try:
                self.producer.produce(message_bytes)
                return
            except KafkaException as e:
                logger.warning(f"Produce failed, reconnecting: {e}")
                self.client   = None
                self.producer = None
                self.connect()


HOSTNAME         = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
kafka_producer   = KafkaProducerWrapper(HOSTNAME, str.encode(app_config['events']['topic']))


def report_match_history(body):
    trace_id = str(uuid.uuid4())
    logger.info(f'Received event match_history with a trace id of {trace_id}')

    for match in body['matches']:
        msg = {
            'type': 'match_history',
            'datetime': datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
            'payload': {
                'user_id':             body['user_id'],
                'reporting_timestamp': body['reporting_timestamp'],
                'match_id':            match['match_id'],
                'champion_id':         match['champion_id'],
                'kills':               match['kills'],
                'deaths':              match['deaths'],
                'win':                 match['win'],
                'trace_id':            trace_id
            }
        }
        kafka_producer.produce(json.dumps(msg).encode('utf-8'))
        logger.info(f'Produced match_history event (trace_id: {trace_id})')

    return NoContent, 201


def report_global_champion_winrates(body):
    trace_id = str(uuid.uuid4())
    logger.info(f'Received event champion_winrate with a trace id of {trace_id}')

    for champion in body['champion_stats']:
        msg = {
            'type': 'champion_winrate',
            'datetime': datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
            'payload': {
                'source_id':           body['source_id'],
                'reporting_timestamp': body['reporting_timestamp'],
                'champion_id':         champion['champion_id'],
                'games_played':        champion['games_played'],
                'win_rate':            champion['win_rate'],
                'patch_version':       body['patch_version'],
                'trace_id':            trace_id
            }
        }
        kafka_producer.produce(json.dumps(msg).encode('utf-8'))
        logger.info(f'Produced champion_winrate event (trace_id: {trace_id})')

    return NoContent, 201


app = connexion.FlaskApp(__name__, specification_dir='')

if "CORS_ALLOW_ALL" in os.environ and os.environ["CORS_ALLOW_ALL"] == "yes":
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
    base_path='/receiver',
    strict_validation=True,
    validate_responses=True
)

if __name__ == '__main__':
    app.run(port=8080, host="0.0.0.0")