import connexion
import json
import yaml
import logging
import logging.config
import datetime
import random
import time
from threading import Thread
from pykafka import KafkaClient
from pykafka.common import OffsetType
from pykafka.exceptions import KafkaException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, MatchHistory, ChampionWinRate

with open('/config/storage_log_config.yml', 'r') as f:
    LOG_CONFIG = yaml.safe_load(f.read())

logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger('basicLogger')

with open('/config/storage_config.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

DB_ENGINE = create_engine(
    f"mysql+mysqldb://{app_config['datastore']['user']}"
    f":{app_config['datastore']['password']}"
    f"@{app_config['datastore']['hostname']}"
    f":{app_config['datastore']['port']}"
    f"/{app_config['datastore']['database']}",
    pool_size=5,
    pool_recycle=1800,
    pool_pre_ping=True
)

Base.metadata.create_all(DB_ENGINE)
Session = sessionmaker(bind=DB_ENGINE)


def get_match_history_readings(start_timestamp, end_timestamp):
    """Returns match history events created between start and end timestamps."""
    session = Session()
    try:
        start_dt = datetime.datetime.fromtimestamp(int(start_timestamp))
        end_dt   = datetime.datetime.fromtimestamp(int(end_timestamp))

        events = session.query(MatchHistory).filter(
            MatchHistory.date_created >= start_dt,
            MatchHistory.date_created < end_dt
        ).all()

        result = [e.to_dict() for e in events]
        logger.info(f"Returning {len(result)} match_history events")
        return result, 200
    finally:
        session.close()


def get_champion_winrate_readings(start_timestamp, end_timestamp):
    """Returns champion winrate events created between start and end timestamps."""
    session = Session()
    try:
        start_dt = datetime.datetime.fromtimestamp(int(start_timestamp))
        end_dt   = datetime.datetime.fromtimestamp(int(end_timestamp))

        events = session.query(ChampionWinRate).filter(
            ChampionWinRate.date_created >= start_dt,
            ChampionWinRate.date_created < end_dt
        ).all()

        result = [e.to_dict() for e in events]
        logger.info(f"Returning {len(result)} champion_winrate events")
        return result, 200
    finally:
        session.close()

class KafkaConsumerWrapper:
    def __init__(self, hostname, topic):
        self.hostname = hostname
        self.topic    = topic      # bytes
        self.client   = None
        self.consumer = None
        self.connect()

    def connect(self):
        """Infinite retry loop — keeps trying until connected."""
        while True:
            logger.debug("Trying to connect to Kafka...")
            if self._make_client():
                if self._make_consumer():
                    logger.info("Kafka consumer ready.")
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
            self.consumer = None
            return False

    def _make_consumer(self):
        if self.consumer is not None:
            return True
        if self.client is None:
            return False
        try:
            topic         = self.client.topics[self.topic]
            self.consumer = topic.get_simple_consumer(
                consumer_group=b'storage_group',
                reset_offset_on_start=False,
                auto_offset_reset=OffsetType.LATEST
            )
            return True
        except KafkaException as e:
            logger.warning(f"Failed to create consumer: {e}")
            self.client   = None
            self.consumer = None
            return False

    def messages(self):
        """
        Generator that yields messages from Kafka.
        If Kafka throws an exception mid-loop, it reconnects and resumes —
        the caller (process_messages) never sees the crash.
        """
        if self.consumer is None:
            self.connect()
        while True:
            try:
                for msg in self.consumer:
                    yield msg
            except KafkaException as e:
                logger.warning(f"Kafka error in consumer loop: {e}")
                self.client   = None
                self.consumer = None
                self.connect()


def process_messages():
    """Background thread: consume from Kafka and write to MySQL."""
    hostname         = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    kafka_consumer   = KafkaConsumerWrapper(hostname, str.encode(app_config['events']['topic']))

    logger.info("Storage: Kafka consumer started")

    for msg in kafka_consumer.messages():
        message = msg.value.decode('utf-8')
        data    = json.loads(message)
        payload = data.get('payload', {})

        session = Session()
        try:
            if data.get('type') == 'match_history':
                event = MatchHistory(
                    user_id             = payload['user_id'],
                    reporting_timestamp = datetime.datetime.strptime(payload['reporting_timestamp'].replace('Z', ''), '%Y-%m-%dT%H:%M:%S'),
                    match_id            = payload['match_id'],
                    champion_id         = payload['champion_id'],
                    kills               = payload['kills'],
                    deaths              = payload['deaths'],
                    win                 = payload['win'],
                    trace_id            = payload['trace_id']
                )
                session.add(event)
                session.commit()
                logger.info(f"Stored match_history event (trace_id: {payload['trace_id']})")

            elif data.get('type') == 'champion_winrate':
                event = ChampionWinRate(
                    source_id           = payload['source_id'],
                    reporting_timestamp = datetime.datetime.strptime(payload['reporting_timestamp'].replace('Z', ''), '%Y-%m-%dT%H:%M:%S'),
                    champion_id         = payload['champion_id'],
                    games_played        = payload['games_played'],
                    win_rate            = payload['win_rate'],
                    patch_version       = payload['patch_version'],
                    trace_id            = payload['trace_id']
                )
                session.add(event)
                session.commit()
                logger.info(f"Stored champion_winrate event (trace_id: {payload['trace_id']})")

        except Exception as e:
            logger.error(f"Error storing event: {e}")
            session.rollback()
        finally:
            session.close()

        kafka_consumer.consumer.commit_offsets()


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('openapi.yaml', strict_validation=True, validate_responses=True)

t = Thread(target=process_messages, daemon=True)
t.start()

if __name__ == '__main__':
    app.run(port=8090, host="0.0.0.0")
