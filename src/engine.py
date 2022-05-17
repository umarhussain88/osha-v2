from dataclasses import dataclass
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from azure.storage.blob import ContainerClient
from io import StringIO
import pandas as pd 
from .utils import logger_util

logger = logger_util(__name__)

@dataclass
class Engine:
    
    """ Engine class for connecting to an Azure SQL Server, 
        Used as a parent class for other classes.
        
        args:
            sql_server: str
            sql_user: str
            sql_db: str
            sql_password: str
        
        usage:
            engine = Engine(sql_server="", sql_user="", sql_db="", sql_password="")
    """

    sql_server: str
    sql_user: str
    sql_password: str
    sql_db: str

    def __post_init__(self):


        params = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={{{self.sql_server}}};DATABASE={{{self.sql_db}}};UID={{{self.sql_user}}};PWD={{{self.sql_password}}};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        con_str = f"mssql+pyodbc:///?odbc_connect={params}"
        engine = create_engine(con_str, fast_executemany=True)
        
        try:
            engine.connect()
        except OperationalError as e: 
            logger.error(f"Failed to connect to database: {e}")
            

        object.__setattr__(self, "engine", engine)
        



@dataclass
class Exporter(Engine):

    """
    Exporter Class for exporting data to Azure Blob Storage
    Inherits from Engine Class which creates a connection to an Azure SQL Server

    Args:
        blob_container_name: Name of the Azure Blob Storage Container
        blob_storage_url: URL of the Azure Blob Storage
    """


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


    blob_container_name : str
    blob_storage_url  : str


    def __post_init__(self):
        
        container_client = ContainerClient.from_connection_string(
            self.blob_storage_url, self.blob_container_name
        )

        object.__setattr__(self, "blob", container_client)


    def write_to_blob_storage(self, filename: str, dataframe: pd.DataFrame) -> None:

        outfile = StringIO
        outfile = dataframe.to_csv(index=False, encoding="utf-8")
        self.blob.upload_blob(
            filename, outfile, overwrite=True, encoding="utf-8")








    