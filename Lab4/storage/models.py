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
