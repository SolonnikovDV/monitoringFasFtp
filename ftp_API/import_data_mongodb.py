import glob
import pymongo
from xml_parse import parse_xml, get_file_list  # type: ignore
# from alive_progress import alive_bar
import PySimpleGUI as sg

import pandas as pd
from pymongo.errors import AutoReconnect

# local import
import config as cfg

PROJECT_PATH = cfg.PROJECT_PATH
# PROJECT_PATH = '/airflow-local/dags/'


def connect_to_mongo():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    # database name
    db_name = client["otus-project"]
    # collection name
    tab_name = db_name["monitoring"]
    return tab_name


def data_import():
    try:
        tab_name = connect_to_mongo()
        # get the list of xml files in 'ftp_files/xml' folder
        _list = get_file_list()
        counter = 0
        # init local metrics for console log out
        total = len(glob.glob(f'{PROJECT_PATH}ftp_files/xml/*'))
        # inserting parsed .xml to db
        # with alive_bar(len(_list), force_tty=True, title=f'Importing...', bar='blocks', ) as bar:
        #     for i, xml_file in enumerate(_list):
        for xml_file in _list:
                dict_data = parse_xml(f'{PROJECT_PATH}ftp_files/xml/{xml_file}')
                tab_name.insert_one(dict_data)
                document = xml_file
                # bar()
                sg.one_line_progress_meter(title='Import to MongoDB progress',
                                           current_value=counter,
                                           max_value=len(_list),
                                           key=f'Now importing >> {document}',
                                           orientation='v')
                counter += 1
        sg.Popup(f'All data were imported.\nTotal {counter} documents')
        # print(f'Inserted {total} .xml documents.')
    except AutoReconnect as e:
        print(e)


def collection_filter():
    tab_name = connect_to_mongo()
    # remove null value
    tab_name.find({'customer': {'$ne': 'null'}})
    print('Documents with null value of "customer" field has been removed')
    # sort collection by date
    tab_name.find().sort('acceptDate', pymongo.ASCENDING)


def drop_data():
    tab_name = connect_to_mongo()
    print(tab_name.name)
    tab_name.delete_many({"customer": "null"})


# not working
def read_mongo():
    tab_name = connect_to_mongo()
    cursor = tab_name.find()
    mongo_doc = list(cursor)

    df = pd.DataFrame(columns=[
        '_id',
        'complaintNumber',
        'acceptDate',
        'decisionPlace',
        'considerationKO',
        'createUser',
        'customer',
        'customer_INN',
        'customer_KPP',
        'applicantNew',
        'purchaseNumber',
        'purchaseCode',
        'purchaseName' ,
        'purchasePlacingDate',
        'purchaseUrl',
    ])

    for num, doc in enumerate(mongo_doc):
        doc['_id'] = str(doc['_id'])
        doc_id = doc['_id']
        series_obj = pd.Series(doc, name=doc_id)
        df = df.append(series_obj)

    df.to_csv(f'{PROJECT_PATH}ftp_files/csv/mongo2csv.csv', index=False,)


if __name__ == '__main__':
    print('Importing data')
    data_import()

    # optional using func, in the demo project all filters were added into UI MongoDb Compass
    # collection_filter()
    # drop_data()
    # read_mongo()