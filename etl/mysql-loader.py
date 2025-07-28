import os
import mysql.connector



def connect_mysql():
    host = os.getenv('MYSQL_HOST', 'localhost')
    port = int(os.getenv('MYSQL_PORT', 3306))
    user = os.getenv('MYSQL_USER', 'etluser')
    password = os.getenv('MYSQL_PASSWORD', 'etlpass')
    database = os.getenv('MYSQL_DB', 'etldb')

    conn = mysql.connector.connect(
        host=host, port=port, user=user, password=password, database=database
    )
    return conn