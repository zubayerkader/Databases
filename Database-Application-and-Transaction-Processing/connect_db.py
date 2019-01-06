import pyodbc

def connect_db():
    ODBC_STR = "Driver={ODBC Driver 17 for SQL Server};Server=tcp:kazi-cmpt354-a5.database.windows.net,1433;Database=CMPT354-A5-DB;Uid=zubayerkader@kazi-cmpt354-a5;Pwd={California12};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    return pyodbc.connect(ODBC_STR)


if __name__ == '__main__':
    print (connect_db())
