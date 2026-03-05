import connexion
from connexion import NoContent
import httpx

STORAGE_MATCH_HISTORY_URL = 'http://localhost:8090/league/match_history'
STORAGE_CHAMPION_WINRATE_URL = 'http://localhost:8090/league/champion_winrate'

def report_match_history(body):
    for match in body['matches']:
        data = {
            'user_id': body['user_id'],
            'batch_timestamp': body['reporting_timestamp'],
            'match_id': match['match_id'],
            'champion_id': match['champion_id'],
            'kills': match['kills'],
            'deaths': match['deaths'],
            'win': match['win']
        }
        r = httpx.post(STORAGE_MATCH_HISTORY_URL, json=data)
    return NoContent, r.status_code

def report_global_champion_winrates(body):
    for champion in body['champion_stats']:
        data = {
            'source_id': body['source_id'],
            'batch_timestamp': body['reporting_timestamp'],
            'champion_id': champion['champion_id'],
            'games_played': champion['games_played'],
            'win_rate': champion['win_rate'],
            'patch_version': body['patch_version']
        }
        r = httpx.post(STORAGE_CHAMPION_WINRATE_URL, json=data)
    return NoContent, r.status_code

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api(
    'openapi.yaml',
    strict_validation=True,
    validate_responses=True
)

if __name__ == '__main__':
    app.run(port=8080)
    