import connexion
from connexion import NoContent
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from models import MatchHistory, ChampionWinRate
import yaml
import logging
import logging.config
from sqlalchemy import select
import json
from threading import Thread
from pykafka import KafkaClient
from pykafka.common import OffsetType

with open('log_conf.yml', 'r') as f:
    LOG_CONFIG = yaml.safe_load(f.read())

logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger('basicLogger')

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

DB_USER     = app_config['datastore']['user']
DB_PASSWORD = app_config['datastore']['password']
DB_HOSTNAME = app_config['datastore']['hostname']
DB_PORT     = app_config['datastore']['port']
DB_NAME     = app_config['datastore']['db']

ENGINE = create_engine(f'mysql://{DB_USER}:{DB_PASSWORD}@{DB_HOSTNAME}:{DB_PORT}/{DB_NAME}')


def make_session():
    return sessionmaker(bind=ENGINE)()


def process_messages():
    """ Process event messages from Kafka """
    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config['events']['topic'])]

    consumer = topic.get_simple_consumer(
        consumer_group=b'event_group',
        reset_offset_on_start=False,
        auto_offset_reset=OffsetType.LATEST
    )

    for msg in consumer:
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        logger.info(f"Message: {msg}")

        payload = msg['payload']

        if msg['type'] == 'match_history':
            session = make_session()
            event = MatchHistory(
                user_id =              payload['user_id'],
                match_id =             payload['match_id'],
                champion_id =          payload['champion_id'],
                kills =                payload['kills'],
                deaths =               payload['deaths'],
                win =                  payload['win'],
                reporting_timestamp =  datetime.strptime(
                    payload['reporting_timestamp'], '%Y-%m-%dT%H:%M:%SZ'
                ),
                trace_id =             payload['trace_id']
            )
            session.add(event)
            session.commit()
            session.close()
            logger.debug(f"Stored match_history event with trace id {payload['trace_id']}")

        elif msg['type'] == 'champion_winrate':
            session = make_session()
            event = ChampionWinRate(
                source_id =            payload['source_id'],
                champion_id =          payload['champion_id'],
                games_played =         payload['games_played'],
                win_rate =             payload['win_rate'],
                patch_version =        payload['patch_version'],
                reporting_timestamp =  datetime.strptime(
                    payload['reporting_timestamp'], '%Y-%m-%dT%H:%M:%SZ'
                ),
                trace_id =             payload['trace_id']
            )
            session.add(event)
            session.commit()
            session.close()
            logger.debug(f"Stored champion_winrate event with trace id {payload['trace_id']}")

        consumer.commit_offsets()


def setup_kafka_thread():
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()


def get_match_history_readings(start_timestamp, end_timestamp):
    session = make_session()
    start = datetime.fromtimestamp(start_timestamp)
    end   = datetime.fromtimestamp(end_timestamp)

    statement = select(MatchHistory).where(
        MatchHistory.date_created >= start,
        MatchHistory.date_created <  end
    )

    results = [
        result.to_dict()
        for result in session.execute(statement).scalars().all()
    ]
    session.close()

    logger.debug('Found %d match history events (start: %s, end: %s)',
                 len(results), start, end)
    return results


def get_champion_winrate_readings(start_timestamp, end_timestamp):
    session = make_session()

    start = datetime.fromtimestamp(start_timestamp)
    end   = datetime.fromtimestamp(end_timestamp)

    statement = select(ChampionWinRate).where(
        ChampionWinRate.date_created >= start,
        ChampionWinRate.date_created <  end
    )

    results = [
        result.to_dict()
        for result in session.execute(statement).scalars().all()
    ]
    session.close()

    logger.debug('Found %d champion winrate events (start: %s, end: %s)',
                 len(results), start, end)
    return results


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('openapi.yaml', strict_validation=True, validate_responses=True)

if __name__ == '__main__':
    setup_kafka_thread()
    app.run(port=8090)
