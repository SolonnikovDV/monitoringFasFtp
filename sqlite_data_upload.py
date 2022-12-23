import sqlite3
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime


# importing data from excel file to sqlite
class ImportDataToDb:
    @classmethod
    def import_data_from_excel(cls, file, db_path, db_table_name):
        df = pd.read_excel(file)
        engine = create_engine(db_path)
        df.to_sql(db_table_name, con=engine, if_exists='replace', index=False)

    @classmethod
    def insert_variable_into_table(
            cls,
            department,
            city_name,
            purchase_number,
            purchase_lot,
            purchase_subject,
            long,
            lat,
            purchase_price,
            complaint_subject,
            year,
            applicant,
            resolution,
            rule_of_law,
            provision_of_law,
            bracking_law_discribe,
            comment,
            penalty_size,
            url_ref):
        try:
            sqlite_connection = sqlite3.connect('monitoring-db.db')
            cursor = sqlite_connection.cursor()
            print("Connected to SQLite")

            sqlite_insert_with_param = """
            INSERT INTO procurement_monitoring(
            department,
            city_name, 
            purchase_number,
            purchase_lot,
            purchase_subject,
            long,
            lat,
            purchase_price,
            complaint_subject,
            year,
            applicant,
            resolution,
            rule_of_law,
            provision_of_law,
            bracking_law_discribe,
            comment,
            penalty_size,
            url_ref) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""

            data_tuple = (
                str(department),
                str(city_name),
                str(purchase_number),
                int(purchase_lot),
                str(purchase_subject),
                float(long),
                float(lat),
                float(purchase_price),
                str(complaint_subject),
                datetime.strptime(year, '%Y-%m-%d'),
                str(applicant),
                str(resolution),
                str(rule_of_law),
                str(provision_of_law),
                str(bracking_law_discribe),
                str(comment),
                float(penalty_size),
                str(url_ref)
            )
            cursor.execute(sqlite_insert_with_param, data_tuple)
            sqlite_connection.commit()
            print("Python Variables inserted successfully into SqliteDb_developers table")

            cursor.close()

        except sqlite3.Error as error:
            print("Failed to insert Python variable into sqlite table", error)
        finally:
            if sqlite_connection:
                sqlite_connection.close()
                print("The SQLite connection is closed")


if __name__ == '__main__':
    print('')
    ImportDataToDb.import_data_from_excel('raw_files/monitoring.xlsx',
                                          'sqlite:///sqlite_db/monitoring.db',
                                          'procurement_monitoring')  # type: ignore
