import psycopg2 as psycopg2
import datetime
import pandas as pd
from sqlalchemy import create_engine
import PySimpleGUI as sg

# local import
import config as cfg
from util import get_loc_geopy

'''
First, create connection to psql
Second, create table in psql DB with columns names
Third, import excel with pandas to psql
'''

DATA_TIME = datetime.datetime.now()
TAB_NAME = 'localmonitoring'
PROJECT_PATH = cfg.PROJECT_PATH


# create connection to psql
def create_connection_to_psql(host, port, dbname, user, password):
    conn = psycopg2.connect(f"""
            host={host}
            port={port}
            dbname={dbname}
            user={user}
            password={password}
            target_session_attrs=read-write
            """)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute('SELECT version()')
        print(f'[INFO] : {DATA_TIME} >> Server version : \n{cur.fetchone()}')
        return conn


# create psql table in DB
def create_table():
    conn = create_connection_to_psql(
        cfg.HOST,
        cfg.PORT,
        cfg.DBNAME,
        cfg.USER,
        cfg.PASSWORD
    )
    try:
        conn.autocommit = True
        # create table
        with conn.cursor() as cur:
            sql_query = f"""
            CREATE TABLE IF NOT EXISTS {TAB_NAME}(
            department text,
            city_name text, 
            purchase_number bigint,
            purchase_lot varchar,
            purchase_subject text,
            long float,
            lat float,
            purchase_price float,
            complaint_subject text,
            year date,
            applicant text,
            resolution text,
            rule_of_law text,
            provision_of_law text,
            bracking_law_discribe text,
            comment text,
            penalty_size float,
            url_ref varchar
            );"""
            cur.execute(sql_query)
            print(f'''[INFO] : {DATA_TIME} >> Table '{TAB_NAME}' created successful''')

    except Exception as e:
        print(f'[INFO] : {DATA_TIME} >> Error creation table : ', e)
    finally:
        if conn:
            conn.close()
            print(f'[INFO] : {DATA_TIME} >> Psql connection closed')


# import excel to psql with pandas
def insert_data_to_table():
    not_found_list = []
    # connecting to psql
    engine = create_engine(
        f'postgresql+psycopg2://{cfg.USER}:{cfg.PASSWORD}@{cfg.HOST}:{cfg.PORT}/{cfg.DBNAME}')
    # read data from excel
    with pd.ExcelFile(f'{PROJECT_PATH}raw_files/monitoring.xlsx') as xls:
        df = pd.read_excel(xls, index_col=0, sheet_name='sheet')
        # append location columns (long, lat) to data frame
        for counter, data in enumerate(df['city_name']):
            # try/catch for unrecognized cities names
            try:
                # extract long/lat from geopy lib to data frame
                if data is not None:
                    print(f'{counter + 1} - {data} location >> {get_loc_geopy(data)}')
                    df.loc[df['city_name'] == data, 'long'] = get_loc_geopy(data).get('long')
                    df.loc[df['city_name'] == data, 'lat'] = get_loc_geopy(data).get('lat')
            except AttributeError as e:
                print(e)
                print(f'lat long for < {data} > are not found')
            # GUI of progress bar
            sg.one_line_progress_meter(title='Getting location info',
                                       current_value=counter,
                                       max_value=len(df['city_name']),
                                       orientation='v')

        print(f'not found {not_found_list}')
        print(df[['city_name', 'long', 'lat']])
        # import data to psql
        df.to_sql(name=TAB_NAME, con=engine, if_exists='replace', index=True)



# TODO push psql db to sqlite
def push_psql_db_to_sqlite():
    ...


if __name__ == '__main__':
    # create_table()
    insert_data_to_table()
