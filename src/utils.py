import logging
from pathlib import Path


def logger_util(name : str) -> logging.Logger:
    logger = logging.getLogger(name)
    
    #create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    #create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(filename)s - %(lineno)d')
    
    #add formatter to ch
    ch.setFormatter(formatter)

    #add ch to logger
    logger.addHandler(ch)

    return logger


def identify_file(file_name : str) -> str:
    """
    Identify the file and write it to the relevant SQL table in stage.
    """
    
    map_dict = {'articles.csv' : 'article', 
     'standards.csv' : 'standard',
     'citations.csv' : 'citation', 
     'phmsa_regulations.csv' : 'phmsa_regulations'
     }
    
    return map_dict.get(file_name)
    
    
    
