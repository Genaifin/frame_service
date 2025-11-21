import json
import pandas as pd
import hashlib
import re
from datetime import datetime, timedelta, timezone

def getStructToFilterLambda(aStruct: dict):
    return {k: lambda x,v=v: x == v for k,v in aStruct.items()}

def getArrayOfStructFromDF(aDF:pd.DataFrame):
    dataAsStruct=json.loads(aDF.to_json(orient='records')) # this convers np.nan to None
    return dataAsStruct

def getFundUniqueId(fundName:str): #TODO move to a proper place (with other param funcs)
    myFundNameToUniqueIdMap={
        'NexBridge':'NexBridge',
        'Altura Strategic Opportunities':'ASOF',
        'Stonewell Diversified':'Stonewell',
    }
    return myFundNameToUniqueIdMap[fundName]

def getFundId(fundName:str):
    myFundNameToIdMap={
        'NexBridge':1,
        'Altura Strategic Opportunities':2,
        'Stonewell Diversified':3,
    }
    return myFundNameToIdMap[fundName]

def getFileHash(file_path, algorithm='sha256'):
    hash_func = getattr(hashlib, algorithm)()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hash_func.update(chunk)
    return hash_func.hexdigest()

def getISO8601FromPDFDate(pdf_date): # TODO cleanup function
    date_str = pdf_date[2:] if pdf_date.startswith('D:') else pdf_date

    match = re.match(r"(\d{14})(Z|([+-])(\d{2})'?(\d{2})'?)?$", date_str)
    if not match:
        try:
            dt = datetime.strptime(pdf_date, "%m/%d/%Y %H:%M:%S")
        except:
            raise ValueError(f"Invalid PDF date format: {pdf_date}")
    else:
        dt_str, _, sign, tz_hour, tz_min = match.groups()
        dt = datetime.strptime(dt_str, "%Y%m%d%H%M%S")

        if tz_hour is not None:
            offset_minutes = int(tz_hour) * 60 + int(tz_min)
            if sign == '-':
                offset_minutes = -offset_minutes
            tz = timezone(timedelta(minutes=offset_minutes))

            dt = dt.replace(tzinfo=tz).astimezone(timezone.utc)

    return dt.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
