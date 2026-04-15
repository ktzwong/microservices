import connexion
import json
import yaml
import logging
import logging.config
import datetime
from threading import Thread
from pykafka import KafkaClient
from pykafka.common import OffsetType
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, MatchHistory, ChampionWinRate

with open('/config/storage_log_config.yml', 'r') as f:
    LOG_CONFIG = yaml.safe_load(f.read())

logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger('basicLogger')

with open('/config/storage_config.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

# Build the DB connection string
DB_ENGINE = create_engine(
    f"mysql+mysqldb://{app_config['datastore']['user']}"
    f":{app_config['datastore']['password']}"
    f"@{app_config['datastore']['hostname']}"
    f":{app_config['datastore']['port']}"
    f"/{app_config['datastore']['database']}"
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


def process_messages():
    """Background thread: consume from Kafka and write to MySQL."""
    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    client   = KafkaClient(hosts=hostname)
    topic    = client.topics[str.encode(app_config['events']['topic'])]
    consumer = topic.get_simple_consumer(
        consumer_group=b'storage_group',
        reset_offset_on_start=False,
        auto_offset_reset=OffsetType.LATEST
    )

    logger.info("Storage: Kafka consumer started")

    for msg in consumer:
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

        consumer.commit_offsets()


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('openapi.yaml', strict_validation=True, validate_responses=True)

t = Thread(target=process_messages, daemon=True)
t.start()

if __name__ == '__main__':
    app.run(port=8090, host="0.0.0.0")
