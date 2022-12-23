from PyQt5.QtCore import Qt
from PyQt5.QtSql import QSqlDatabase, QSqlTableModel, QSqlRecord
from PyQt5.QtWidgets import (QApplication, QMainWindow, QMessageBox, QTableView, QPushButton, QVBoxLayout,
                             QWidget, QHBoxLayout)
# from webtest import app
import config as cfg
from ftp_API.open_bi import open_metabase
from ftp_API.ftp_get_data import download_data
import sys, os
from sqlite_data_upload import ImportDataToDb


class SqlTableWindow(QMainWindow):
    def __init__(self, parent=None):
        super().__init__(parent)
        # self.setWindowTitle('SQL Table')
        self.resize(1000, 400)

        # Set up model
        self.model = QSqlTableModel(self)

        # code for sqlite connect
        self.model.setTable(cfg.dB_table_name)
        # code for psql connect
        # self.model.setTable(f'monitoring.{cfg.dB_table_name}')

        self.model.setEditStrategy(QSqlTableModel.OnFieldChange)
        # Headers
        self.model.setHeaderData(0, Qt.Horizontal, 'department'),
        self.model.setHeaderData(1, Qt.Horizontal, 'city_name'),
        self.model.setHeaderData(2, Qt.Horizontal, 'purchase_number'),
        self.model.setHeaderData(3, Qt.Horizontal, 'purchase_lot'),
        self.model.setHeaderData(4, Qt.Horizontal, 'purchase_subject'),
        self.model.setHeaderData(5, Qt.Horizontal, 'long'),
        self.model.setHeaderData(6, Qt.Horizontal, 'lat'),
        self.model.setHeaderData(7, Qt.Horizontal, 'purchase_price'),
        self.model.setHeaderData(8, Qt.Horizontal, 'complaint_subject'),
        self.model.setHeaderData(9, Qt.Horizontal, 'year'),
        self.model.setHeaderData(10, Qt.Horizontal, 'applicant'),
        self.model.setHeaderData(11, Qt.Horizontal, 'resolution'),
        self.model.setHeaderData(12, Qt.Horizontal, 'rule_of_law'),
        self.model.setHeaderData(13, Qt.Horizontal, 'provision_of_law'),
        self.model.setHeaderData(14, Qt.Horizontal, 'bracking_law_discribe'),
        self.model.setHeaderData(15, Qt.Horizontal, 'comment'),
        self.model.setHeaderData(16, Qt.Horizontal, 'penalty_size'),
        self.model.setHeaderData(17, Qt.Horizontal, 'url_ref')
        self.model.select()
        # Set up view
        self.view = QTableView()
        self.view.setModel(self.model)
        self.view.columnWidth(30)
        self.setCentralWidget(self.view)
        # button

    def add_row(self):
        rowCount = self.model.rowCount()
        self.model.insertRow(rowCount)
        print(f'add row to : {rowCount}')

    def remove_row(self):
        if self.model.rowCount() > 0:
            self.model.removeRow(self.model.rowCount() - 1)
            print(f'remove row to : {self.model.rowCount() - 1}')

    # @staticmethod
    def import_excel(self):
        data_frame = ImportDataToDb.import_data_from_excel('raw_files/monitoring.xlsx',
                                                           'sqlite:///sqlite_db/monitoring.db',
                                                           'procurement_monitoring')
        QMessageBox.information(None,
                                "Data import status",
                                f'Excel {cfg.import_excel_table_name} import is successful')
        print(f'Excel {cfg.import_excel_table_name} import is successful')
        return data_frame


class AppTable(QWidget):
    def __init__(self):
        super().__init__()
        self.resize(1600, 600)
        self.setWindowTitle('SQL Monitoring Table')

        mainLayout = QHBoxLayout()
        table = SqlTableWindow()
        mainLayout.addWidget(table)
        buttonLayout = QVBoxLayout()

        button_new = QPushButton('New')
        button_new.clicked.connect(table.add_row)
        buttonLayout.addWidget(button_new)

        button_remove = QPushButton('Remove')
        button_remove.clicked.connect(table.remove_row)
        buttonLayout.addWidget(button_remove, alignment=Qt.AlignTop)

        button_download_api = QPushButton('Download from FAS')
        button_download_api.clicked.connect(lambda: download_data())
        buttonLayout.addWidget(button_download_api, alignment=Qt.AlignTop)

        button_import_excel = QPushButton('Import Excel')
        button_import_excel.clicked.connect(lambda: table.import_excel())
        buttonLayout.addWidget(button_import_excel)

        button_bi_analytics = QPushButton('BI local analytics')
        button_bi_analytics.clicked.connect(lambda: open_metabase(cfg.LOCAL_ANALYTIC_META))
        buttonLayout.addWidget(button_bi_analytics)

        button_bi_analytics_global = QPushButton('BI global analytics')
        button_bi_analytics_global.clicked.connect(lambda: open_metabase(cfg.GLOB_ANALYTIC_META))
        buttonLayout.addWidget(button_bi_analytics_global)

        mainLayout.addLayout(buttonLayout)
        self.setLayout(mainLayout)


def create_connection_psql():
    db = QSqlDatabase.addDatabase('QPSQL')
    db.setHostName('localhost')
    db.setPort(5432)
    db.setDatabaseName('monitoring')
    db.setUserName('user')
    db.setPassword('pass')
    print("Available drivers", db.drivers())
    db.open()
    if not db.open():
        print("Unable to connect.")
        print('Last error', db.lastError().text())
    else:
        print("Connection to the database successful")
    return True


def create_connection_sqlite():
    db = QSqlDatabase.addDatabase('QSQLITE')
    db.setDatabaseName('sqlite_db/monitoring.db')
    db.databaseName()
    db.connectionName()
    print(db.connectionName())
    db.open()
    if not db.open():
        QMessageBox.critical(None,
                             "App Name - Error!",
                             "Database Error: %s" % db.lastError().databaseText(), )
        sys.exit(1)


def run_app():
    app = QApplication(sys.argv)
    app.setStyleSheet('QPushButton{font-size: 15px; width: 150px; height: 25px}')
    # if not create_connection():
    #     sys.exit(1)
    # create_connection_sqlite()
    create_connection_psql()
    win = AppTable()
    win.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    app = QApplication(sys.argv)
