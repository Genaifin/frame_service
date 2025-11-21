from frontendUtils.renders.utils.pageConfig import getPageConfig
from frontendUtils.renders.utils.pageParameters import getParameters
import  frontendUtils.renders.frame.fileExtractionViewer as frameRenders
import  frontendUtils.renders.frame.dashboardStats as frameDashboardRenders

from fastapi import HTTPException


async def getModuleRender(params:dict):
    querySubType=params['query'].get('_subType',None)
    globalFuncMap=await moduleRenderFuncs()

    if querySubType is None:
        raise HTTPException(status_code=400, detail="_subType is required")
    
    if querySubType not in globalFuncMap:
        raise HTTPException(status_code=400, detail=f"_subType {querySubType} is invalid")
    
    myFuncMap=globalFuncMap[querySubType]
    
    queryFuncName=params['query'].get('_funcName',None)
    if queryFuncName is None:
        raise HTTPException(status_code=400, detail="_funcName is required")

    if queryFuncName not in myFuncMap:
        raise HTTPException(status_code=400, detail=f"_funcName {queryFuncName} is invalid")

    queryFunc=myFuncMap[queryFuncName]
    # Check if function is async and await it
    import asyncio
    import inspect
    if inspect.iscoroutinefunction(queryFunc):
        return await queryFunc(params)
    else:
        return queryFunc(params)    

async def moduleRenderFuncs():
    return {
        'singleFundCompare':{
            'fileValidationSummaryTable':singleFundCompareRenders.getFileValidations,
            'fileValidationSummarySubPage':singleFundCompareRenders.getFileValidationsSubPage,
            'pricingValidationsLevel2Table':singleFundCompareRenders.getPricingValidationsLevel2Table,
            'NAVValidationDetailsTabs':singleFundCompareRenders.getNAVValidationDetailsTabs,
            'NAVValidationDetailsConfig':singleFundCompareRenders.getNAVValidationDetailsConfig,
            'ratioValidationsPageCombinedOutput':singleFundCompareRenders.getRatioValidationsPageCombinedOutput,
            'singleFundPageParameters':paramSelectorRenders.getParamSelector,
            'singleFundComparePageCombinedOutput':singleFundCompareRenders.getSingleFundComparePageCombinedOutput,
            'navValidationsPageCombinedOutput':singleFundCompareRenders.getNAVValidationsPageCombinedOutput,
            'checkPointsCombinedOutput': singleFundCompareRenders.getCheckPointsCombinedOutput,
            'reportViewData':singleFundCompareRenders.getReportViewData,
        },
        'frame':{
            'allFilesTable':frameRenders.getAllFilesTable,
            'allFilesTabs':frameRenders.getAllFilesTabs,
            'getAllExtractedDataFor1File':frameRenders.getGetAllExtractedDataFor1File,
            'getTotalReceivedFilesCount':frameDashboardRenders.getTotalReceivedFilesCount,
            'getProcessedFilesCount':frameDashboardRenders.getProcessedFilesCount,
            'getInProgressFilesCount':frameDashboardRenders.getInProgressFilesCount,
            'getFailedFilesCount':frameDashboardRenders.getFailedFilesCount,
            'getIgnoredFilesCount':frameDashboardRenders.getIgnoredFilesCount,
            'getDeliveryStats':frameDashboardRenders.getDeliveryStats,
            'getFileCountsByStatus':frameDashboardRenders.getFileCountsByStatus,
            'getInReviewFilesCount':frameDashboardRenders.getInReviewFilesCount,
            'getDuplicateFilesCount':frameDashboardRenders.getDuplicateFilesCount,
            'getProcessedFilesProgressBar':frameDashboardRenders.getProcessedFilesProgressBar,
            'getFailedFilesProgressBar':frameDashboardRenders.getFailedFilesProgressBar,
            'getInReviewFilesProgressBar':frameDashboardRenders.getInReviewFilesProgressBar,
            'getDuplicateFilesProgressBar':frameDashboardRenders.getDuplicateFilesProgressBar,
            'getInProgressFilesProgressBar':frameDashboardRenders.getInProgressFilesProgressBar,
        },
        'configs':{
            'pageConfig':getPageConfig,
            'parameters':getParameters
        }
    }
