import json
import logging
import os

import azure.functions as func
import pandas as pd
from bs4 import BeautifulSoup
from src import Clean, Engine, logger_util

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


    if file_name == "articles.csv":

        article_df = cl.get_article_from_html(blob_stream)
        article_df = cl.strip_title(article_df)
        article_df["content"] = article_df["content"].apply(
            lambda x: BeautifulSoup(x, "html.parser")
        )
        article_df["content"] = article_df["content"].apply(cl.remove_ul_header)
        article_df.to_sql(
            "letters_of_interpretation", schema="stg", index=False, con=eng.engine, 
            if_exists='replace'
        )

    elif file_name == "standards.csv":
        logger.info('Processing "standards.csv"')
        standards_df = cl.standards_dataframe(blob_stream)
        logging.info(f"Dataframe shape: {standards_df.shape}")

        standards_df["created_date"] = pd.Timestamp('now').strftime('%Y-%m-%d %H:%M:%S')
        
        standards_df.to_sql(
            name="loi_documents",
            schema="stg",
            con=eng.engine,
            if_exists="replace",
            index=False,
        )
