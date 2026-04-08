import connexion
from connexion import NoContent
import json
import os
from datetime import datetime


MAX_BATCH_EVENTS = 5
MATCH_HISTORY_FILE = "match_history.json"
CHAMPION_WINRATE_FILE = "champion_winrate.json"


def report_match_history(body):
    """
    Receives a batch of match history data and stores it in a JSON file.
    Keeps track of the last 5 batches and total count.
    """
    total_kills = 0
    num_matches = len(body['matches'])
    
    for match in body['matches']:
        total_kills += match['kills']
    
    avg_kills = total_kills / num_matches 
    
    if os.path.exists(MATCH_HISTORY_FILE):
        with open(MATCH_HISTORY_FILE, 'r') as f:
            data = json.load(f)
    else:
        data = {
            "num_match_history_batches": 0,
            "recent_batch_data": []
        }
    
    data["num_match_history_batches"] += 1
    
    new_batch = {
        "avg_kills": round(avg_kills, 2),
        "num_matches": num_matches,
        "received_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    }
    
 
    data["recent_batch_data"].append(new_batch)
    
    if len(data["recent_batch_data"]) > MAX_BATCH_EVENTS:
        data["recent_batch_data"].pop(0)  
    
    with open(MATCH_HISTORY_FILE, 'w') as f:
        json.dump(data, f, indent=2)
    return NoContent, 201


def report_global_champion_winrates(body):
    """
    Receives a batch of champion win rate data and stores it in a JSON file.
    Keeps track of the last 5 batches and total count.
    """
    total_win_rate = 0
    num_champions = len(body['champion_stats'])
    
    for champion in body['champion_stats']:
        total_win_rate += champion['win_rate']
    
    avg_win_rate = total_win_rate / num_champions 
    
    if os.path.exists(CHAMPION_WINRATE_FILE):
        with open(CHAMPION_WINRATE_FILE, 'r') as f:
            data = json.load(f)
    else:
        data = {
            "num_champion_winrate_batches": 0,
            "recent_batch_data": []
        }
   
    data["num_champion_winrate_batches"] += 1
    
    new_batch = {
        "avg_win_rate": round(avg_win_rate, 4),
        "num_champions": num_champions,
        "received_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    }
    
    data["recent_batch_data"].append(new_batch)
    
    if len(data["recent_batch_data"]) > MAX_BATCH_EVENTS:
        data["recent_batch_data"].pop(0)  
    
    with open(CHAMPION_WINRATE_FILE, 'w') as f:
        json.dump(data, f, indent=2)
    return NoContent, 201

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    app.run(port=8080)