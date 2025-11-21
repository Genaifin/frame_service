import re
from datetime import datetime
import pandas as pd

def fileNameMatches(aFileName: str,aCustomFileIdentifier: dict,aDate: str,aDateFormat: str):
    myDateStr = convertDateToFormat(aDate,aDateFormat)
    if myDateStr in aFileName:
        if aCustomFileIdentifier['type']=='strBegins':
            return aFileName.startswith(aCustomFileIdentifier['value'])
        elif aCustomFileIdentifier['type']=='strContains':
            return aCustomFileIdentifier['value'] in aFileName
        elif aCustomFileIdentifier['type']=='strEnds':
            return aFileName.endswith(aCustomFileIdentifier['value'])
    return False

def findDateInString(aString,dateFormat):
    allFormats=dateFormats()
    if dateFormat in allFormats:
        myFormatMeta=allFormats[dateFormat]
        
        matches=re.findall(myFormatMeta['regex'], aString)
        if len(matches)!=1:
            # trackAssertions('preliminary','file date verify',len(matches)==1,'(%s) for file %s'%(dateFormat,aString))
            raise Exception( 'could not get date from %s %s' %(aString,dateFormat))

        myMatch=matches[0]
        myDate=datetime.strptime(myMatch, myFormatMeta['convertFormat'])
        if myFormatMeta.get('applyOffset',False):
            myDate= (myDate+pd.offsets.MonthEnd(0))

        myDateStr=myDate.date().isoformat()
        return myDateStr
    else:
        raise Exception('unexpected date format %s'%(dateFormat))
    
def convertDateToFormat(aDate: str,aDateFormat: str):
    allFormats=dateFormats()
    if aDateFormat in allFormats:
        myFormatMeta=allFormats[aDateFormat]
        return datetime.strptime(aDate,allFormats['YYYY-MM-DD']['convertFormat']).strftime(myFormatMeta['convertFormat'])
    else:
        raise Exception('unexpected date format %s'%(aDateFormat))

def dateFormats():
    formats={
        'MMM DD YYYY':{
            'regex':r'\b([A-Za-z]{3} \d{2} \d{4})\b',
            'convertFormat':'%b %d %Y'
        },
        'YYYY-MM-DD':{
            # 'regex':r'\b\d{4}-\d{2}-\d{2}\b',
            'regex':r"(?:^|[. _\w-])(\d{4}-\d{2}-\d{2})(?=[. _\w-]|$)",
            'convertFormat':'%Y-%m-%d'
        },
        'MM.DD.YYYY':{
            'regex':r'\b\d{2}\.\d{2}\.\d{4}\b',
            'convertFormat':'%m.%d.%Y'
        },    
        'YYYYMMDD':{
            'regex':r"(?:^|[.\s_])(\d{8})(?=[.\s_]|$)",
            'convertFormat':'%Y%m%d'
        },
        'MMDDYYYY':{
            'regex':r"(?:^|[.\s_\w])(\d{8})(?=[.\s_\w]|$)",
            'convertFormat':'%m%d%Y'
        },
        'MM-DD-YY':{
            'regex':r"(?:^|[. _])(\d{2}-\d{2}-\d{2})(?=[. _]|$)",
            'convertFormat':'%m-%d-%y'
        },
        'MM-DD-YYYY':{
            'regex':r"(?:^|[. _])(\d{2}-\d{2}-\d{4})(?=[. _]|$)",
            'convertFormat':'%m-%d-%Y'
        },
        'MMM YYYY':{
            'regex':r"(?:^|[.\s_\w])([A-Za-z]{3} \d{4})(?=[.\s_\w]|$)",
            'convertFormat':'%b %Y',
            'applyOffset':True
        },
        
    }
    return formats