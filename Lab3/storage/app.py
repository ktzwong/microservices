import connexion
from connexion import NoContent
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from models import MatchHistory, ChampionWinRate

ENGINE = create_engine("sqlite:///league.db")

def make_session():
    return sessionmaker(bind=ENGINE)()


def save_match_history(body):
    session = make_session()

    reporting_timestamp = datetime.strptime(
        body["reporting_timestamp"], "%Y-%m-%dT%H:%M:%SZ"
    )

    event = MatchHistory(
        user_id=body["user_id"],
        match_id=body["match_id"],
        champion_id=body["champion_id"],
        kills=body["kills"],
        deaths=body["deaths"],
        win=body["win"],
        reporting_timestamp=reporting_timestamp
    )

    session.add(event)
    session.commit()
    session.close()
    return NoContent, 201


def save_champion_winrate(body):
    session = make_session()

    reporting_timestamp = datetime.strptime(
        body["reporting_timestamp"], "%Y-%m-%dT%H:%M:%SZ"
    )

    event = ChampionWinRate(
        source_id=body["source_id"],
        champion_id=body["champion_id"],
        games_played=body["games_played"],
        win_rate=body["win_rate"],
        patch_version=body["patch_version"],
        reporting_timestamp=reporting_timestamp
    )

    session.add(event)
    session.commit()
    session.close()
    return NoContent, 201


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('openapi.yaml', strict_validation=True, validate_responses=True)

if __name__ == '__main__':
    app.run(port=8090)