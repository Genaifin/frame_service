import importlib
def getLambdaFromString(aCilentName: str,aLambdaString: str):
    if aLambdaString.__class__.__name__=='function':
        return aLambdaString
    if aLambdaString.__class__.__name__=='str':
        if aLambdaString.startswith('CF::'):
            moduleName=aLambdaString[4:].split('.')[0]
            functionName=aLambdaString[4:].split('.')[1]
            return getattr(importlib.import_module("clients."+aCilentName+".customFunctions."+moduleName),functionName)
        if aLambdaString.startswith('BOX::'):
            boxScope=aLambdaString[5:].split('.')[0]
            moduleName=aLambdaString[5:].split('.')[1]
            className=aLambdaString[5:].split('.')[2]
            return getattr(importlib.import_module("boxes."+boxScope+"."+moduleName),className)
        else:
            return eval(aLambdaString)
    else:
        raise Exception(f"Unknown lambda type {aLambdaString.__class__.__name__}")