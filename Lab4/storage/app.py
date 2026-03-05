import connexion
from connexion import NoContent
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from models import MatchHistory, ChampionWinRate
import yaml
import logging
import logging.config

with open('log_conf.yml', 'r') as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger('basicLogger')


with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

DB_USER = app_config['datastore']['user']
DB_PASSWORD = app_config['datastore']['password']
DB_HOSTNAME = app_config['datastore']['hostname']
DB_PORT = app_config['datastore']['port']
DB_NAME = app_config['datastore']['db']

ENGINE = create_engine(f'mysql://{DB_USER}:{DB_PASSWORD}@{DB_HOSTNAME}:{DB_PORT}/{DB_NAME}')


def make_session():
    return sessionmaker(bind=ENGINE)()

def save_match_history(body):
    session = make_session()
    event = MatchHistory(
        user_id = body['user_id'],
        match_id = body['match_id'],
        champion_id = body['champion_id'],
        kills = body['kills'],
        deaths = body['deaths'],
        win = body['win'],
        reporting_timestamp = datetime.strptime(body['reporting_timestamp'], '%Y-%m-%dT%H:%M:%SZ'),
        trace_id = body['trace_id']   
    )
    session.add(event)
    session.commit()
    session.close()
    return NoContent, 201

def save_champion_winrate(body):
    session = make_session()
    event = ChampionWinRate(
        source_id = body['source_id'],
        champion_id = body['champion_id'],
        games_played = body['games_played'],
        win_rate = body['win_rate'],
        patch_version = body['patch_version'],
        reporting_timestamp = datetime.strptime(body['reporting_timestamp'], '%Y-%m-%dT%H:%M:%SZ'),
        trace_id = body['trace_id']   
    )
    session.add(event)
    session.commit()
    session.close()
    logger.debug(f"Stored event champion_winrate with a trace id of {body['trace_id']}")
    return NoContent, 201


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('openapi.yaml', strict_validation=True, validate_responses=True)

if __name__ == '__main__':
    app.run(port=8090)
