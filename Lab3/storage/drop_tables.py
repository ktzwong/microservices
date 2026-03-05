from sqlalchemy import create_engine
from models import Base

ENGINE = create_engine("sqlite:///league.db")

if __name__ == "__main__":
    Base.metadata.drop_all(ENGINE)
    print("Tables dropped successfully")
