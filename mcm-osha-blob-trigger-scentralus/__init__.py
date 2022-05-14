import logging

import azure.functions as func
from src import Engine, Clean
import os
import pandas as pd
import json




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

    # read blob stream as json object
    blob_stream = myblob.read()
    blob_json = json.loads(blob_stream)

    # convert json object to pandas dataframe

    #write a function to parse the file name and then execute the correct function. 

    standards_df = cl.standards_dataframe(blob_json)
    logging.info(f"Dataframe shape: {standards_df.shape}")

    standards_df['Process'] = -1
    standards_df['CreatedDate'] = pd.Timestamp.now()
    standards_df['UpdatedDate'] = pd.Timestamp.now()
    standards_df['externalID'] = -1

    standards_df.to_sql(name="LoiDocuments",schema='dbo', con=eng.engine, if_exists="replace", index=False)


