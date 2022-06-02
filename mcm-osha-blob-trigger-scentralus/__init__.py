import json
import logging
import os

import azure.functions as func
import pandas as pd
from bs4 import BeautifulSoup
from src import Clean, Engine, logger_util, identify_file

logger = logger_util(__name__)


def main(myblob: func.InputStream):
    logging.info(
        f"Python blob trigger function processed blob \n"
        f"Name: {myblob.name}\n"
        f"Blob Size: {myblob.length} bytes"
    )

    # get file name
    eng = Engine(
        sql_server=os.environ.get("osha_sql_server"),
        sql_db=os.environ.get("osha_sql_db"),
        sql_user=os.environ.get("osha_sql_user"),
        sql_password=os.environ.get("osha_sql_password"),
    )

    cl = Clean()

    file_name = myblob.name.split("/")[-1]
    logging.info(f"File name: {file_name}")

    # read blob stream as csv object

    blob_stream = myblob.read()

    table = identify_file(file_name)

    df = cl.create_csv_dataframe_from_stream(blob_stream)

    df.to_sql(table, schema="stg", con=eng.engine, if_exists="replace", index=False)
