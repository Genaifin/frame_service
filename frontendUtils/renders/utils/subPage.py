

cssPropertiesByName={
    "toBeNamedSubPage": {
            "gap": "20px",
            "padding": "24px",
            "borderRadius": "24px",
                "backgroundColor": "white"
        }
}


def getSubPageRender(modules,cssPropertiesName):
    myRender={
        "modules": modules,
        "cssProperties": getCSSPropertiesFromName(cssPropertiesName)
    }
    return myRender

def getCSSPropertiesFromName(cssPropertiesName):
    if cssPropertiesName not in cssPropertiesByName:
        raise ValueError(f"CSS property {cssPropertiesName} not found")
    return cssPropertiesByName[cssPropertiesName]


