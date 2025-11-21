from rbac.utils.frontend import getUserPreferences

from fastapi.responses import JSONResponse

async def getUserMetaResponse(username: str):
    myResponse = getUserPreferences(username)
    return JSONResponse(content=myResponse)