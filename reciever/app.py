import connexion
from connexion import NoContent
import httpx
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

    for match in body['matches']:
        data = {
            'user_id':          body['user_id'],
            'batch_timestamp':  body['reporting_timestamp'],
            'match_id':         match['match_id'],
            'champion_id':      match['champion_id'],
            'kills':            match['kills'],
            'deaths':           match['deaths'],
            'win':              match['win'],
            'trace_id':         trace_id
        }
        r = httpx.post(app_config['eventstore1']['url'], json=data)

    logger.info(f'Response for event match_history (id: {trace_id}) has status {r.status_code}')
    return NoContent, r.status_code


def report_global_champion_winrates(body):
    trace_id = str(uuid.uuid4())
    logger.info(f'Received event champion_winrate with a trace id of {trace_id}')

    for champion in body['champion_stats']:
        data = {
            'source_id':        body['source_id'],
            'batch_timestamp':  body['reporting_timestamp'],
            'champion_id':      champion['champion_id'],
            'games_played':     champion['games_played'],
            'win_rate':         champion['win_rate'],
            'patch_version':    champion['patch_version'],
            'trace_id':         trace_id
        }
        r = httpx.post(app_config['eventstore2']['url'], json=data)

    logger.info(f'Response for event champion_winrate (id: {trace_id}) has status {r.status_code}')
    return NoContent, r.status_code


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('openapi.yaml', strict_validation=True, validate_responses=True)

if __name__ == '__main__':
    app.run(port=8080)
