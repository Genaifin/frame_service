import pandas as pd
from validations import VALIDATION_STATUS
from utils.unclassified import getArrayOfStructFromDF

def validateDFSize(df:pd.DataFrame,expectedSize:int,productName:str,type:str,subType:str,subType2:str):
    if len(df)!=expectedSize:
        return VALIDATION_STATUS().setProductName(productName).setType(type).setSubType(subType).setSubType2(subType2).setMessage(-len(df)).setData({'rows':getArrayOfStructFromDF(df)})
    else:
        return VALIDATION_STATUS().setProductName(productName).setType(type).setSubType(subType).setSubType2(subType2).setMessage(0)
    
def validateDFSizeWithThreshold(df:pd.DataFrame,expectedSize:int,productName:str,type:str,subType:str,subType2:str,aThreshold:float):
    if len(df)!=expectedSize:
        return VALIDATION_STATUS().setProductName(productName).setType(type).setSubType(subType).setSubType2(subType2).setMessage(-len(df)).setData({'rows':getArrayOfStructFromDF(df)}).setThreshold(aThreshold)
    else:
        return VALIDATION_STATUS().setProductName(productName).setType(type).setSubType(subType).setSubType2(subType2).setMessage(0).setThreshold(aThreshold)
    
def validateNonZeroDFSize(df:pd.DataFrame,productName:str,type:str,subType:str,subType2:str):
    if len(df)==0:
        return VALIDATION_STATUS().setProductName(productName).setType(type).setSubType(subType).setSubType2(subType2).setMessage(-1)
    else:
        return VALIDATION_STATUS().setProductName(productName).setType(type).setSubType(subType).setSubType2(subType2).setMessage(0)

def countNoneFields(obj, node_name=None):
    """
    If node_name is provided, return the total count of all items in the 'rows' field under the 'data' key of every dict where subType2 == node_name (recursively).
    If node_name is None, recursively count the number of fields with value None in a dict or list.
    Returns an int.
    """
    if node_name is not None:
        rows_count = 0
        if isinstance(obj, dict):
            if (
                obj.get('subType2') == node_name and
                'data' in obj and
                obj['data'] and
                isinstance(obj['data'], dict) and
                'rows' in obj['data'] and
                isinstance(obj['data']['rows'], list)
            ):
                rows_count += len(obj['data']['rows'])
            for v in obj.values():
                rows_count += countNoneFields(v, node_name)
        elif isinstance(obj, list):
            for item in obj:
                rows_count += countNoneFields(item, node_name)
        return rows_count
    else:
        none_count = 0
        if isinstance(obj, dict):
            for v in obj.values():
                if v is None:
                    none_count += 1
                else:
                    none_count += countNoneFields(v)
        elif isinstance(obj, list):
            for item in obj:
                none_count += countNoneFields(item)
        return none_count

