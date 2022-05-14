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
