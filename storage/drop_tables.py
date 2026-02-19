# create_tables.py
import yaml
from sqlalchemy import create_engine
from models import Base

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

DB_USER     = app_config['datastore']['user']
DB_PASSWORD = app_config['datastore']['password']
DB_HOSTNAME = app_config['datastore']['hostname']
DB_PORT     = app_config['datastore']['port']
DB_NAME     = app_config['datastore']['db']

ENGINE = create_engine(
    f'mysql://{DB_USER}:{DB_PASSWORD}@{DB_HOSTNAME}:{DB_PORT}/{DB_NAME}'
)

if __name__ == '__main__':
    Base.metadata.drop_all(ENGINE)
    print('Tables dropped')
