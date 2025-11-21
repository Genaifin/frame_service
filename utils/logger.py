import logging
logging.basicConfig(
    format='%(asctime)s:%(funcName)s %(levelname)s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

def getLogger(name:str):
    if name in getLogger._cache:
        return getLogger._cache[name]
    logger=logging.getLogger(name)
    getLogger._cache[name]=logger
    return logger
getLogger._cache={}


