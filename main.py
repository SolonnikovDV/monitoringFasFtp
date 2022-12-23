# import main_table
import table_window
import sys
from PyQt5 import QtWidgets, QtCore



app = QtWidgets.QApplication(sys.argv)
print("PluginsPath =>          " + QtCore.QLibraryInfo.location(QtCore.QLibraryInfo.PluginsPath))

print(sys.version)

table_window.run_app()
