from sqlalchemy.orm import DeclarativeBase, mapped_column
from sqlalchemy import Integer, String, DateTime, Float, Boolean, func

class Base(DeclarativeBase):
    pass

class MatchHistory(Base):
    __tablename__ = "match_history"

    id = mapped_column(Integer, primary_key=True)
    user_id = mapped_column(String(250), nullable=False)
    match_id = mapped_column(String(250), nullable=False)
    champion_id = mapped_column(Integer, nullable=False)
    kills = mapped_column(Integer, nullable=False)
    deaths = mapped_column(Integer, nullable=False)
    win = mapped_column(Boolean, nullable=False)
    reporting_timestamp = mapped_column(DateTime, nullable=False)
    date_created = mapped_column(DateTime, nullable=False,default=func.now())
    trace_id = mapped_column(String(250), nullable=False)

    def to_dict(self):
        return {
            'id':                   self.id,
            'user_id':              self.user_id,
            'match_id':             self.match_id,
            'champion_id':          self.champion_id,
            'kills':                self.kills,
            'deaths':               self.deaths,
            'win':                  self.win,
            'reporting_timestamp':  self.reporting_timestamp.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'trace_id':             self.trace_id,
            'date_created':         self.date_created.strftime('%Y-%m-%dT%H:%M:%SZ')
            }

class ChampionWinRate(Base):
    __tablename__ = "champion_winrate"

    id = mapped_column(Integer, primary_key=True)
    source_id = mapped_column(String(250), nullable=False)
    champion_id = mapped_column(Integer, nullable=False)
    games_played = mapped_column(Integer, nullable=False)
    win_rate = mapped_column(Float, nullable=False)
    patch_version = mapped_column(String(50), nullable=False)
    reporting_timestamp = mapped_column(DateTime, nullable=False)
    date_created = mapped_column(DateTime, nullable=False,default=func.now())
    trace_id = mapped_column(String(250), nullable=False)

    def to_dict(self):
        return {
            'id':                   self.id,
            'source_id':            self.source_id,
            'champion_id':          self.champion_id,
            'games_played':         self.games_played,
            'win_rate':             self.win_rate,
            'patch_version':        self.patch_version,
            'reporting_timestamp':  self.reporting_timestamp.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'trace_id':             self.trace_id,
            'date_created':         self.date_created.strftime('%Y-%m-%dT%H:%M:%SZ')
            }
