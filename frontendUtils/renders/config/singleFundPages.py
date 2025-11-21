from frontendUtils.renders.classes.pageLayout import SIMPLE_MODULE,SUB_PAGE,TEXT_HEADER_SUBPAGE,SINGLE_MODULE_SUBPAGE
from frontendUtils.renders.classes.pageLayout import BREADCRUMB,ATHENA_TOGGLE,NOTIFICATION_ICON,SUBPAGE_SUBPAGE,PAGE_CONFIG

from frontendUtils.renders.classes.pageLayout import TAB_CONTENT,SEARCH_SUBPAGE,UPLOAD_FILES,BREADCRUMB_TITLE_ONLY
from frontendUtils.renders.classes.pageLayout import BUTTON_WITH_ACTION,FILE_MANAGER_GROUPING,MENU_ICONS,OVERRIDDEN_MODULE
pages={

    'singleFundCompare':PAGE_CONFIG(
        [
            BREADCRUMB_TITLE_ONLY("Controller's Dashboard"),  
            UPLOAD_FILES(),
            ATHENA_TOGGLE(),
            NOTIFICATION_ICON(),
        ],
        [
            [SIMPLE_MODULE("_validusSF_dynamicFilters",{"paramName": "validus_singleFund"},12,4)],
            [
                # EMPTY_SPACE(6,4),
                SIMPLE_MODULE("_validusSF_statsRepresentation",{"_funcName": "singleFundComparePageCombinedOutput"},4,2,["NAVValueSourceA"],"statsRepresentation"),
                SIMPLE_MODULE("_validusSF_statsRepresentation",{"_funcName": "singleFundComparePageCombinedOutput"},4,2,["NAVValueSourceB"],"statsRepresentation"),
                # Removed: RNAV Value After Edits
            ],
            [TAB_CONTENT(
                {
                    "Validations":SUB_PAGE([
                            SUBPAGE_SUBPAGE([
                                TEXT_HEADER_SUBPAGE("Validation Summary",'default',100,"_validus_TextHeader"),
                                SINGLE_MODULE_SUBPAGE('_validusSF_groupedStatsCard',{"_funcName": "singleFundComparePageCombinedOutput"},100,82,["summaryStatsCard"]),
                            ],50,43,'default',"_validus_subPageFromConfig"),
                            SUBPAGE_SUBPAGE([
                                TEXT_HEADER_SUBPAGE("Reports ingested",'default',94,"_validus_TextHeader"),
                                OVERRIDDEN_MODULE({
                                    "name": "ExpandFiles",
                                    "moduleType": "filters",
                                    "filters": [
                                        {
                                            "type": "expand",
                                            "label": "Sources",
                                            "key": "source",
                                            "clickAction": {
                                                "type": "navigation",
                                                "to": "/validus",
                                                "parameters": [
                                                    {
                                                        "key": "page",
                                                        "value": "ReportsIngested"
                                                    }
                                                ]
                                            }
                                        }
                                    ],
                                    "cssProperties": {
                                        "justifyContent": "flex-start"
                                    }
                                }, 4, None),
                                SINGLE_MODULE_SUBPAGE('_validusSF_nestedTable',{"_funcName": "singleFundComparePageCombinedOutput"},100,80,["fileValidationSummaryTable"]),
                            ],50,43,'default',"_validus_subPageFromConfig"),
                            SINGLE_MODULE_SUBPAGE('ValidationCheckpoints',{"_funcName": "singleFundComparePageCombinedOutput"},50,55,['checkPoints']),
                            SUBPAGE_SUBPAGE([
                                TEXT_HEADER_SUBPAGE("Validation Matrix",'default',100,"_validus_TextHeader"),
                                SINGLE_MODULE_SUBPAGE('_validusSF_groupedStatsCard',{"_funcName": "singleFundComparePageCombinedOutput"},100,85,['dataValidations']),
                            ],50,55,'default',"_validus_subPageFromConfig"),
                        ],0,0,"gap10px","_validus_subPageFromConfig"),
                    "Trends":SIMPLE_MODULE('SingleFundTrends',{},0,0)
                },
                12,43,"_validusSF_TabContent"
            )]
        ],
        ["ProcessableFiles", "FilesProjections"]
    ),
    'ratio-validations':PAGE_CONFIG(
        [
            BREADCRUMB(
                "Ratio Health Checks",
                [
                    {"name":"VALIDUS"},
                    {
                        "name":"Single Fund",
                        "route":"/validus?page=singleFundCompare"
                    }
                ],True
            ),
            NOTIFICATION_ICON(),
        ],
        [
            [SIMPLE_MODULE("_validusSF_dynamicFilters",{"paramName": "validus_singleFund"},12,4)],
            [
                SIMPLE_MODULE("_validusSF_statCard",{"_funcName": "ratioValidationsPageCombinedOutput"},4,6,["totalRatios"]),
                SIMPLE_MODULE("_validusSF_statCard",{"_funcName": "ratioValidationsPageCombinedOutput"},4,6,["majorDeviation"]),
                SIMPLE_MODULE("_validusSF_statCard",{"_funcName": "ratioValidationsPageCombinedOutput"},4,6,["minorDeviation"]),
            ],
            [
                SUB_PAGE([
                    SINGLE_MODULE_SUBPAGE('_validusSF_nestedTable',{"_funcName": "ratioValidationsPageCombinedOutput"},100,100,['ratioValidationsTable'])
                ],12,50,'default')
            ]
        ],
    ),
    'nav-validations':PAGE_CONFIG(
        [
            BREADCRUMB(
                "NAV Validations",
                [
                    {"name":"VALIDUS"},
                    {
                        "name":"Single Fund",
                        "route":"/validus?page=singleFundCompare"
                    }
                ],True
            ),
            NOTIFICATION_ICON(),
        ],
        [
            [SIMPLE_MODULE("_validusSF_dynamicFilters",{"paramName": "validus_singleFund"},12,4)],
            [
                SIMPLE_MODULE("_validusSF_statCard",{"_funcName": "navValidationsPageCombinedOutput"},3,2,["totalValidations"]),
                SIMPLE_MODULE("_validusSF_statCard",{"_funcName": "navValidationsPageCombinedOutput"},3,2,["totalPassed"]),
                SIMPLE_MODULE("_validusSF_statCard",{"_funcName": "navValidationsPageCombinedOutput"},3,2,["totalFailed"]),
                SIMPLE_MODULE("_validusSF_statCard",{"_funcName": "navValidationsPageCombinedOutput"},3,2,["totalExceptions"]),
            ],
            [
                SUB_PAGE([
                    SINGLE_MODULE_SUBPAGE('_validusSF_nestedTable',{"_funcName": "navValidationsPageCombinedOutput"},100,100,['pnlValidationsLevel1Table'])
                ],12,59,'default')
            ]
        ],
    ),
    'nav-validation-details':PAGE_CONFIG(
        [
            BREADCRUMB(
                "NAV Validations Details",
                [
                    {"name":"VALIDUS"},
                    {
                        "name":"Single Fund",
                        "route":"/validus?page=singleFundCompare"
                    },
                    {
                        "name": "NAV Validations",
                        "route": "/validus?page=nav-validations"
                    }
                ],True
            ),
            NOTIFICATION_ICON(),
        ],
         [
             [SIMPLE_MODULE("_validusSF_dynamicFilters",{"paramName": "validus_singleFund"},12,4)],
             [SIMPLE_MODULE("_validusSF_statsRepresentation",{"_funcName": "navValidationsPageCombinedOutput"},12,2,["NAVValueAfterEdits"])],
             [
                 SUB_PAGE([
                     TEXT_HEADER_SUBPAGE("NAV Validations Details",'default',60),
                     OVERRIDDEN_MODULE({
                         "moduleType": "toggle",
                         "label": "Show non-exceptions",
                         "clickAction": {
                             "type": "storeValues",
                             "store": {
                                 "key": "filterTable",
                                 "value": "isException"
                             }
                         }
                     }, 19, None),
                     SEARCH_SUBPAGE(20,None),
                     SINGLE_MODULE_SUBPAGE('SingleSourceTabs',{},100,93),
                 ],12,45,'default'),
             ]
         ],
    ),
    'data-validations':PAGE_CONFIG(
        [
            BREADCRUMB(
                "Validation Matrix",
                [
                    {"name":"VALIDUS"},
                    {
                        "name":"Single Fund",
                        "route":"/validus?page=singleFundCompare"
                    }
                ],True
            ),
            NOTIFICATION_ICON(),
        ],
        [
            [SIMPLE_MODULE("_validusSF_dynamicFilters",{"paramName": "validus_singleFund"},12,4)],
            [
                SUB_PAGE([
                    TEXT_HEADER_SUBPAGE("Validation matrix",'default',80),
                    SEARCH_SUBPAGE(20,None),
                    SINGLE_MODULE_SUBPAGE('DataValidationTabs',{},100,93),
                ],12,45,'default'),
            ]
        ],
    ),    
    'FileManagerNew':PAGE_CONFIG(
        [
            BREADCRUMB_TITLE_ONLY("File Manager"),
            BUTTON_WITH_ACTION("Clear Files", {"type": "clearFiles"}),
            UPLOAD_FILES(),
            NOTIFICATION_ICON(),
        ],
        [
            [
                SUB_PAGE([
                    FILE_MANAGER_GROUPING(),
                    MENU_ICONS(),
                    SINGLE_MODULE_SUBPAGE('allFilesTabs', {"_subType": "frame", "_funcName": "allFilesTabs"}, 100, 93),
                ], 12, 45, 'default')
            ]
        ]
    ),

}    