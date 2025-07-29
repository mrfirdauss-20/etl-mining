from lib import DatabaseConnector, ETLPipeline
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

mysql_config = {
    "host": os.getenv("MYSQL_HOST"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASS"),
    "database": os.getenv("MYSQL_DB"),
    "port": int(os.getenv("MYSQL_PORT")),
}

doris_config = {
    "host": os.getenv("DORIS_HOST"),
    "user": os.getenv("DORIS_USER"),
    "password": os.getenv("DORIS_PASS"),
    "database": os.getenv("DORIS_DB"),
    "port": int(os.getenv("DORIS_PORT")),
}

doris_raw_adapter = DatabaseConnector.MySQLAdapter(doris_config)
doris_raw_adapter.connect()
mysql_adapter = DatabaseConnector.MySQLAdapter(mysql_config)
mysql_adapter.connect()

dt_str = datetime.today().strftime("%Y-%m-%d")
pipeline = ETLPipeline(dt=dt_str, doris_adapter=doris_raw_adapter, mysql_adapter=mysql_adapter)
pipeline.run()

pipeline.cleanup()
