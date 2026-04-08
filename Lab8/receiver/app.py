import connexion
from connexion import NoContent
import json
import datetime
from pykafka import KafkaClient
import yaml
import logging
import logging.config
import uuid

with open('log_conf.yml', 'r') as f:
    LOG_CONFIG = yaml.safe_load(f.read())

logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger('basicLogger')

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())


def report_match_history(body):
    trace_id = str(uuid.uuid4())
    logger.info(f'Received event match_history with a trace id of {trace_id}')

    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config['events']['topic'])]
    producer = topic.get_sync_producer()

    for match in body['matches']:
        msg = {
            'type': 'match_history',
            'datetime': datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
            'payload': {
                'user_id':               body['user_id'],
                'reporting_timestamp':   body['reporting_timestamp'],
                'match_id':              match['match_id'],
                'champion_id':           match['champion_id'],
                'kills':                 match['kills'],
                'deaths':                match['deaths'],
                'win':                   match['win'],
                'trace_id':              trace_id
            }
        }
        msg_str = json.dumps(msg)
        producer.produce(msg_str.encode('utf-8'))
        logger.info(f'Produced match_history event (trace_id: {trace_id})')

    return NoContent, 201


def report_global_champion_winrates(body):
    trace_id = str(uuid.uuid4())
    logger.info(f'Received event champion_winrate with a trace id of {trace_id}')

    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config['events']['topic'])]
    producer = topic.get_sync_producer()

    for champion in body['champion_stats']:
        msg = {
            'type': 'champion_winrate',
            'datetime': datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
            'payload': {
                'source_id':             body['source_id'],
                'reporting_timestamp':   body['reporting_timestamp'],
                'champion_id':           champion['champion_id'],
                'games_played':          champion['games_played'],
                'win_rate':              champion['win_rate'],
                'patch_version':         body['patch_version'],
                'trace_id':              trace_id
            }
        }
        msg_str = json.dumps(msg)
        producer.produce(msg_str.encode('utf-8'))
        logger.info(f'Produced champion_winrate event (trace_id: {trace_id})')

    return NoContent, 201


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('openapi.yaml', strict_validation=True, validate_responses=True)

if __name__ == '__main__':
    app.run(port=8080, host="0.0.0.0")
