from rbac.utils.frontend import getUserProfileSectionData

class PAGE_CONFIG():
    def __init__(self,topNavBarParams:list,moduleDisplayConfig:list,subPages:list=None):
        self.topNavBarParams=topNavBarParams
        self.moduleDisplayConfig=moduleDisplayConfig
        self.subPages=subPages if subPages is not None else []
    
    def getFrontendConfig(self,username:str):
        myConfig={
            "topNavBarParams":self._convertNAVParamsToFrontEndConfig(self.topNavBarParams,username),
            "moduleDisplayConfig":self._convertToFrontEndConfig(self.moduleDisplayConfig),
        }
        if self.subPages:
            myConfig["subPages"] = self.subPages
        return myConfig
    
    def _convertToFrontEndConfig(self,aModuleDisplayConfig):
        myConfig=[]
        currentY=0
        currentId=0
        for row in aModuleDisplayConfig:
            currentX=0
            maxY=0
            for myModule in row:
                myModuleConfig,currentX,currentId=myModule.getFrontendConfig(currentX,currentY,currentId)
                maxY=max(maxY,myModule.h)
                if myModuleConfig != {}:
                    myConfig.append(myModuleConfig)

            if currentX>12:
                raise Exception('width for row %s %.2f'%(str([ aModule.metricName for aModule in row]),currentX))

            currentY+=maxY
        return myConfig

    def _convertNAVParamsToFrontEndConfig(self,aNAVParams,username:str):
        myConfig=[]
        for aNAVParam in aNAVParams:
            myConfig.append(aNAVParam.getFrontendParams(username))
        return myConfig
    
class SUBPAGE_CONFIG():
    def __init__(self,aSubPage):
        self.subPage=aSubPage

    def getFrontendConfig(self,username:str):
        myConfig,_a,_b= self.subPage.getFrontendConfig(-1,0,0)
        return myConfig

class TOP_NAV_BAR():
    def __init__(self):
        pass

    def getFrontendParams(self,username:str):
        raise NotImplementedError("Subclasses must implement this method")
    
class BREADCRUMB(TOP_NAV_BAR):
    def __init__(self,aPageTitle:str,aBreadcrumbs:list,showBackButton:bool):
        self.aPageTitle=aPageTitle
        self.aBreadcrumbs=aBreadcrumbs
        self.showBackButton=showBackButton

    def getFrontendParams(self,username:str):
        myMeta={
            "moduleName":"breadcrumb",
            "isShow": True,
            "data":{
                "title":self.aPageTitle,
                "breadcrumb":self.aBreadcrumbs,
                "isShowBackButton":self.showBackButton
            },
        }
        return myMeta
    
class BREADCRUMB_TITLE_ONLY(TOP_NAV_BAR): # why is it different from BREADCRUMB()?
    def __init__(self,aPageTitle:str):
        self.aPageTitle=aPageTitle

    def getFrontendParams(self,username:str):
        myMeta={
            "moduleName":"breadcrumb",
            "isShow": True,
            "data":{
                "title":self.aPageTitle
            }
        }
        return myMeta
    
class NOTIFICATION_ICON(TOP_NAV_BAR):
    def getFrontendParams(self,username:str):
        myMeta={
            "moduleName":"notificationIcon",
            "isShow": True
        }
        return myMeta
    
class UPLOAD_FILES(TOP_NAV_BAR):
    def getFrontendParams(self,username:str):
        myMeta={
            "moduleName": "uploadfiles",
            "isShow": True,
            "data": {
                "label": "Upload files"
            }
        }
        return myMeta
    
class ATHENA_TOGGLE(TOP_NAV_BAR):
    def getFrontendParams(self,username:str):
        myMeta={
            "moduleName": "athenaToggle",
            "isShow": True,
            "data": {
                "isSelected": False,
                "label": "Athena Mode",
                "config":"singleFundCompareAthenaMode"
            }
        }
        return myMeta
    
class PROFILE_SECTION(TOP_NAV_BAR):
    def getFrontendParams(self,username:str):
        myMeta={
            "isShow": True,
            "moduleName":"profileSection",
            "data":getUserProfileSectionData(username)
        }
        return myMeta

class MODULE_RENDER:
    def __init__(self,w,h):   
        self.w=w
        self.h=h   
    def getFrontendConfig(self,startX,Y):
        raise NotImplementedError("Subclasses must implement this method")
    
    def _newX(self,startX):
        return startX+self.w
    
    def _newId(self,id):
        return id+1
    
    def _getLayout(self,startX,Y):
        return { "x": startX, "y": Y, "w": self.w, "h": self.h}
    
    def _addLayoutAndId(self,aConfig,startX,Y,id):
        if startX ==-1:
            return aConfig
        aConfig["layout"]=self._getLayout(startX,Y)
        aConfig["id"]=id
        return aConfig

class TAB_CONTENT(MODULE_RENDER):
    def __init__(self,tabsWithName,w,h,name=None):
        self.tabsWithName=tabsWithName
        self.w=w
        self.h=h
        self.name=name

    def getFrontendConfig(self,startX,Y,id):
        first=True
        myTabsOutput=[]
        for aTabName,tabClass in self.tabsWithName.items():
            myConfig,_,_=tabClass.getFrontendConfig(-1,0,0)
            myConfig['nameOfTab']=aTabName
            myConfig['isActive']=first
            first=False
            myTabsOutput.append(myConfig)

        overridenModuleMeta={
            'moduleType':'tabContent',
            "tabs":myTabsOutput,
            "cssProperties":{
                "height":"100%"
            },
            "type":"secondary"
        }
        if self.name:
            overridenModuleMeta['name'] = self.name
        myConfig={
            'overridenModuleMeta': overridenModuleMeta,
        }
        myConfig=self._addLayoutAndId(myConfig,startX,Y,id)
        return myConfig,self._newX(startX),self._newId(id)

class EMPTY_SPACE(MODULE_RENDER):
    def __init__(self,w,h):
        self.w=w
        self.h=h

    def getFrontendConfig(self,startX,Y,id):
        return {},self._newX(startX),self._newId(id)

class SIMPLE_MODULE(MODULE_RENDER):
    def __init__(self,moduleName,overriddenParam,w,h,overridePathToPickup=[],overriddenModuleType=None):   
        self.moduleName=moduleName
        self.overriddenParam=overriddenParam
        self.w=w
        self.h=h
        self.overridePathToPickup=overridePathToPickup
        self.overriddenModuleType=overriddenModuleType

        # assert self.metricName in getSingleFundFuncMap(), '%s is not an activeFunc'%(metricName)
    def getFrontendConfig(self,startX,Y,id):
        myConfig={
            "moduleName": self.moduleName,
            "overrridenParam": self.overriddenParam,
        }
        if self.overriddenModuleType:
            myConfig["overriddenModuleType"] = self.overriddenModuleType
        if len(self.overridePathToPickup)>0:
            myConfig["overridePathToPickup"]=self.overridePathToPickup
        myConfig=self._addLayoutAndId(myConfig,startX,Y,id)

        return myConfig,self._newX(startX),self._newId(id)
    
subPageCSSConfigs={
    'default':{
        "gap": "12px",
        "backgroundColor": "white",
        "borderRadius": "24px",
        "padding": "24px"
    },
    'defaultNoGap':{
        "backgroundColor": "white",
        "borderRadius": "24px",
        "padding": "24px",
    },
    "gap10px":{
        "gap": "10px",
    }
}

class SUB_PAGE(MODULE_RENDER):
    def __init__(self,aModules,w,h,CSSConfigName='default',name=None):
        self.modules=aModules
        self.w=w
        self.h=h
        self.CSSConfigName=CSSConfigName
        self.name=name

    def getFrontendConfig(self,startX,Y,id):
        overridenModuleMeta={
            'moduleType':'subPage',
            "modules":[module.getFrontendConfigSubpage() for module in self.modules],
            "cssProperties": subPageCSSConfigs[self.CSSConfigName],
        }
        if self.name:
            overridenModuleMeta['name'] = self.name
        myConfig={
            'overridenModuleMeta': overridenModuleMeta,
        }
        myConfig=self._addLayoutAndId(myConfig,startX,Y,id)
        return myConfig,self._newX(startX),self._newId(id)
    
headerCSSConfigs={
    'default':{
        "fontSize": "16px",
        "fontWeight": "600",
        "textTransform": "uppercase",
        "color": "#475569",
        "padding": "24px",
        "borderRadius": "24px"
    },
    'fileManager':{
        "fontSize": "16px",
        "fontWeight": "600",
        "textTransform": "uppercase",
        "color": "#4b5563"
    },
}

def getHeaderCSSConfig(CSSConfigName):
    if CSSConfigName not in headerCSSConfigs:
        raise ValueError(f"Invalid CSS config name: {CSSConfigName}")
    return headerCSSConfigs[CSSConfigName]

class MODULE_SUBPAGE_RENDER():
    def __init__(self,widthPct,heightPct):
        self.widthPct=widthPct
        self.heightPct=heightPct

    def addHeightAndWidth(self,aConfig):
        if self.widthPct is not None:
            aConfig['width']=f"{int(self.widthPct)}%"
        if self.heightPct is not None:
            aConfig['height']=f"{int(self.heightPct)}%"
        return aConfig
    
    def getFrontendConfigSubpage(self):
        raise NotImplementedError("Subclasses must implement this method")

class TEXT_HEADER_SUBPAGE(MODULE_SUBPAGE_RENDER):
    def __init__(self,headerText,CSSConfigName,widthPct,name=None):
        self.headerText=headerText
        self.CSSConfigName=CSSConfigName
        self.widthPct=widthPct
        self.heightPct=None
        self.name=name

    def getFrontendConfigSubpage(self):
        overridenModuleMeta={
            'moduleType':'textHeader',
            "header": self.headerText,
            "cssProperties": getHeaderCSSConfig(self.CSSConfigName)
        }
        if self.name:
            overridenModuleMeta['name'] = self.name
        myConfig={
            'overridenModuleMeta': overridenModuleMeta
        }
        return self.addHeightAndWidth(myConfig)
    
class SINGLE_MODULE_SUBPAGE(MODULE_SUBPAGE_RENDER):
    def __init__(self,moduleName,overriddenParam,widthPct,heightPct,overridePathToPickup=[]):   
        self.moduleName=moduleName
        self.overriddenParam=overriddenParam
        self.widthPct=widthPct
        self.heightPct=heightPct
        self.overridePathToPickup=overridePathToPickup

    def getFrontendConfigSubpage(self):
        myConfig={
            'moduleName':self.moduleName
        }
        if len(self.overriddenParam)>0:
            myConfig["overrridenParam"]=self.overriddenParam
        if len(self.overridePathToPickup)>0:
            myConfig["overridePathToPickup"]=self.overridePathToPickup

        return self.addHeightAndWidth(myConfig)
    
class SUBPAGE_SUBPAGE(MODULE_SUBPAGE_RENDER): #TODO Rename
    def __init__(self,modules,widthPct,heightPct,CSSConfigName='defaultNoGap',name=None):
        self.modules=modules
        self.widthPct=widthPct
        self.heightPct=heightPct
        self.CSSConfigName=CSSConfigName
        self.name=name

    def getFrontendConfigSubpage(self):
        overridenModuleMeta={
            'moduleType':'subPage',
            'modules':[module.getFrontendConfigSubpage() for module in self.modules],
            "cssProperties": subPageCSSConfigs[self.CSSConfigName]
        }
        if self.name:
            overridenModuleMeta['name'] = self.name
        myConfig={
            'overridenModuleMeta': overridenModuleMeta
        }
        return self.addHeightAndWidth(myConfig)
    
class SEARCH_SUBPAGE(MODULE_SUBPAGE_RENDER):
    def __init__(self,widthPct,heightPct):
        self.widthPct=widthPct
        self.heightPct=heightPct

    def getFrontendConfigSubpage(self):
        myConfig={
            "overridenModuleMeta":{
                "moduleType":"searchModule",
                "placeholder": "Search",
                "cssProperties": {
                    "height": "32px"
                }
            }
        }   
        return self.addHeightAndWidth(myConfig)

class OVERRIDDEN_MODULE(MODULE_SUBPAGE_RENDER): 
    def __init__(self, overridenModuleMeta, widthPct, heightPct):
        self.overridenModuleMeta = overridenModuleMeta
        self.widthPct = widthPct
        self.heightPct = heightPct
    
    def getFrontendConfigSubpage(self):
        myConfig = {
            "overridenModuleMeta": self.overridenModuleMeta
        }
        return self.addHeightAndWidth(myConfig)

class FILE_MANAGER_GROUPING(MODULE_SUBPAGE_RENDER):
    def __init__(self, label="Grouping", widthPct=17):
        self.label = label
        self.widthPct = widthPct
        self.heightPct = None
    
    def getFrontendConfigSubpage(self):
        myConfig = {
            "overridenModuleMeta": {
                "name": "FileManagerGrouping",
                "moduleType": "filters",
                "data": {
                    "label": self.label
                },
                "filters": [
                    {
                        "type": "dateRangePicker"
                    },
                    {
                        "type": "drawer",
                        "key": "filters"
                    }
                ],
                "cssProperties": {
                    "gap": "2px",
                    "justifyContent": "flex-end",
                    "height": "32px"
                }
            }
        }
        return self.addHeightAndWidth(myConfig)

class MENU_ICONS(MODULE_SUBPAGE_RENDER):    
    def __init__(self, widthPct=5, icons=None):
        self.widthPct = widthPct
        self.heightPct = None
        # Default icons if none provided
        self.icons = icons or [
            {"type": "eye", "key": "eye"},
            {"type": "menu", "key": "menu"}
        ]
    
    def getFrontendConfigSubpage(self):
        myConfig = {
            "overridenModuleMeta": {
                "name": "MenuIcons",
                "moduleType": "menuIcons",
                "filters": self.icons,
                "cssProperties": {
                    "gap": "2px",
                    "justifyContent": "flex-end",
                    "height": "32px"
                }
            }
        }
        return self.addHeightAndWidth(myConfig)

class BUTTON_WITH_ACTION(TOP_NAV_BAR):
    def __init__(self, buttonText: str, action: dict, buttonType: str = "primary"):
        self.buttonText = buttonText
        self.action = action
        self.buttonType = buttonType 
    def getFrontendParams(self, username: str):  
        return {
            "moduleName": "button",
            "isShow": True,
            "data": {
                "buttonText": self.buttonText,
                "action": self.action,
                "type": self.buttonType,
            },
        }
