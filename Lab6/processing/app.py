import connexion
import json
import os
import requests
import logging
import logging.config
import yaml
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler

with open('log_conf.yml', 'r') as f:
    LOG_CONFIG = yaml.safe_load(f.read())
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger('basicLogger')

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())


def get_stats():
    logger.info('Request received for GET /stats')
    filename = app_config['datastore']['filename']

    if not os.path.isfile(filename):
        logger.error('Statistics file %s does not exist', filename)
        return {'message': 'Statistics do not exist'}, 404

    with open(filename, 'r') as f:
        stats = json.load(f)

    logger.debug('Statistics retrieved: %s', stats)
    logger.info('Request for GET /stats completed')

    return {
        'num_match_readings':   stats['num_match_readings'],
        'max_kills':            stats['max_kills'],
        'num_winrate_readings': stats['num_winrate_readings'],
        'max_win_rate':         stats['max_win_rate']
    }, 200


def populate_stats():
    logger.info('Start processing')

    filename = app_config['datastore']['filename']

    if os.path.isfile(filename):
        with open(filename, 'r') as f:
            stats = json.load(f)
    else:
        stats = {
            'num_match_readings':   0,
            'max_kills':            0,
            'num_winrate_readings': 0,
            'max_win_rate':         0.0,
            'last_updated':         0   
        }

    start_timestamp = stats['last_updated']
    end_timestamp   = int(datetime.now().timestamp())
    params = {'start_timestamp': start_timestamp, 'end_timestamp': end_timestamp}

    match_url = app_config['eventstores']['match_history']['url']
    r1 = requests.get(match_url, params=params)
    if r1.status_code == 200:
        match_events = r1.json()
        logger.info('Received %d new match history events', len(match_events))
    else:
        logger.error('Failed to get match history events. Status: %d', r1.status_code)
        match_events = []

    winrate_url = app_config['eventstores']['champion_winrate']['url']
    r2 = requests.get(winrate_url, params=params)
    if r2.status_code == 200:
        winrate_events = r2.json()
        logger.info('Received %d new champion winrate events', len(winrate_events))
    else:
        logger.error('Failed to get champion winrate events. Status: %d', r2.status_code)
        winrate_events = []


    stats['num_match_readings']   += len(match_events)
    stats['num_winrate_readings'] += len(winrate_events)

    for event in match_events:
        if event['kills'] > stats['max_kills']:
            stats['max_kills'] = event['kills']

    for event in winrate_events:
        if event['win_rate'] > stats['max_win_rate']:
            stats['max_win_rate'] = event['win_rate']

    stats['last_updated'] = end_timestamp

    with open(filename, 'w') as f:
        json.dump(stats, f, indent=4)

    logger.debug('Updated statistics: %s', stats)
    logger.info('END periodic processing')


def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(
        populate_stats,
        'interval',
        seconds=app_config['scheduler']['interval']
    )
    sched.start()

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('openapi.yaml', strict_validation=True, validate_responses=True)

if __name__ == '__main__':
    init_scheduler()
    app.run(port=8100)