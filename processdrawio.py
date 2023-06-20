import configparser
from lxml import objectify,etree
import hashlib
from copy import deepcopy
class InteractiveDrawIO:
    CONFIG_FILE_NAME = "layout.config"
    def __init__(self,configPath):
        self.configPath = configPath
        self.config = configparser.SafeConfigParser()

    def __readConfigFile__(self):
        self.config.read("{}/{}".format(self.configPath, self.CONFIG_FILE_NAME))



def getObjName(columnObject):
    if "name" in columnObject.attrib:
        return columnObject.attrib["name"]
    else:
        return columnObject.attrib["label"]

def getTargetTableObject(mxfile,targetName):
    for obj in mxfile.diagram.mxGraphModel.root.iterchildren(tag='UserObject'):
        if "vertex" in obj.mxCell.attrib and obj.mxCell.attrib['vertex'] == "1":
            if "name" in obj.attrib:
                if obj.attrib['name'] == targetName:
                    return obj
            else:
                if obj.attrib['label'] == targetName:
                    return obj
    return None


"""
    ls = []
    for obj in mxfile.diagram.mxGraphModel.root.iterchildren(tag='UserObject'):
        if "vertex" in obj.mxCell.attrib and obj.mxCell.attrib['vertex'] == "1":
            if obj.mxCell.attrib['parent'] == targetTableObj.attrib["id"]:
                ls.append(obj)
    return ls
"""

def getTargetColumnsObject(mxfile,targetTableObj):
    id = targetTableObj.attrib["id"]
    predicate = "UserObject/mxCell[@vertex='1'][@parent='{}']/..".format(id)
    cols = mxfile.diagram.mxGraphModel.root.iterfind(predicate)
    return list(cols)


def duplicateColumns(mxfile,columnsList):
    for col in columnsList:
        elem = deepcopy(col)
        elem.attrib["id"] = "cloned_" + elem.attrib["id"]

        elem.attrib["tags"]="sel_"+getObjName(elem)
        col.attrib["tags"] = "norm_" + getObjName(elem)

        elem.mxCell.attrib['visible']="0"
        col.mxCell.attrib['visible'] = "1"

        elem = setStyle(elem,{"gradientColor":"#ffa500"})

        (normAction,selAction) = createAction(getObjName(elem),{"gradientColor":"#b3b3b3"},{"gradientColor": "#ffa500"})
        elem.attrib["link"]=normAction
        col.attrib["link"]=selAction
        #par = col.getparent()
        mxfile.diagram.mxGraphModel.root.insert(mxfile.diagram.mxGraphModel.root.index(col)+1,elem)
        #mxfile.diagram.mxGraphModel.root.append(elem)
    return mxfile



def setStyle(columnObj,stylesDict):
    style = columnObj.mxCell.attrib["style"]
    stList = style[:-1].split(";")
    stDict = {}
    for st in stList:
        keyval = st.split("=")
        if len(keyval) < 2:
            stDict[keyval[0]]=""
        else:
            stDict[keyval[0]]=keyval[1]
    for key in stylesDict.keys():
        stDict[key] = stylesDict[key]
    strStyle = ""
    for key in stDict.keys():
        if key == "noEdgeStyle":
            continue
        elif key == "edgeStyle":
            strStyle += "{}={};".format(key, "entityRelationEdgeStyle")
            continue
        elif len(stDict[key]) == 0:
            strStyle += "{};".format(key)
        else:
            strStyle+="{}={};".format(key,stDict[key])

    columnObj.mxCell.attrib["style"] = strStyle
    return columnObj

def createAction(destColName,normStyles,selStyles):
    """
    Get list of tags and create te selct and normal action
    """

    show_norm   = '{{"show":{{"tags":["norm_{}"]}}}}'.format(destColName)
    show_sel    = '{{"show":{{"tags":["sel_{}"]}}}}'.format(destColName)
    hide_norm   = '{{"hide":{{"tags":["norm_{}"]}}}}'.format(destColName)
    hide_sel    = '{{"hide":{{"tags":["sel_{}"]}}}}'.format(destColName)

    show_norm_arrows = '{"show":{"tags":["norm_arrows"]}}'
    show_sel_arrows = '{"show":{"tags":["sel_arrows"]}}'
    hide_norm_arrows = '{"hide":{"tags":["norm_arrows"]}}'
    hide_sel_arrows = '{"hide":{"tags":["sel_arrows"]}}'

    style_norm = []
    for key in normStyles.keys():
        temp = '{{"style":{{"tags":["src_{}"],"key":"{}","value":"{}"}}}}'.format(destColName,key,normStyles[key])
        style_norm.append(temp)

    final_norm_style = ",".join(style_norm)
    style_sel = []
    for key in selStyles.keys():
        temp = '{{"style":{{"tags":["src_{}"],"key":"{}","value":"{}"}}}}'.format(destColName, key,selStyles[key])
        style_sel.append(temp)
    final_sel_style = ",".join(style_sel)

    action_show_norm = 'data:action/json,{{"actions":[{},{},{},{},{}]}}'.format(show_norm,show_norm_arrows,hide_sel,hide_sel_arrows,final_norm_style)
    action_show_sel = 'data:action/json,{{"actions":[{},{},{},{}]}}'.format(show_sel, hide_norm,hide_norm_arrows, final_sel_style)
    return (action_show_norm,action_show_sel)





    #### create dict and set the corresponding styles from stylesDict
    #then set it in the duplciated column
    # create action to show and hide normal cols an edges and selected cols and edges +
    #set style of the source cols to be colored or return to normal
    """
    set new style in duplicated column
    set new style in duplicated edge
     
    """




def getEdgeListTargetColumn(mxfile,columnObject):
    colId = columnObject.attrib["id"]
    predicate = "UserObject/mxCell[@edge='1'][@target='{}']/..".format(colId)
    edges = mxfile.diagram.mxGraphModel.root.iterfind(predicate)
    return edges

def addTagToSourceColumn(mxfile,srcColumnId,destColumnObject):
    predicate = "UserObject[@id = '{}']/mxCell[@vertex='1']/..".format(srcColumnId)
    srcCol = list(mxfile.diagram.mxGraphModel.root.iterfind(predicate))
    srcCol=srcCol[0]
    if "tags" in srcCol.attrib:
        tags = srcCol.attrib["tags"]
    else:
        tags = ""
    tgls = tags.split(" ")
    tgls.append("src_"+getObjName(destColumnObject))
    if len(tgls[0]) == 0:
        tgls.pop(0)
    srcCol.attrib["tags"] = " ".join(tgls)


def duplicateEdgesForColumn(mxfile,columnObject):
    edges = list(getEdgeListTargetColumn(mxfile,columnObject))
    for edge in edges:
        srcId = edge.mxCell.attrib["source"]
        addTagToSourceColumn(mxfile,srcId,columnObject)
        elem = deepcopy(edge)
        elem.attrib["id"] = "cloned_" + elem.attrib["id"]
        elem.attrib["tags"] = "sel_" + getObjName(columnObject)+" sel_arrows"
        edge.attrib["tags"] = "norm_" + getObjName(columnObject)+" norm_arrows"
        elem = setStyle(elem, {"strokeColor": "#f51919"})
        edge = setStyle(edge, {"strokeColor": "#000000"})
        elem.mxCell.attrib['visible'] = "0"
        edge.mxCell.attrib['visible'] = "1"
        elem.mxCell.attrib["target"] = "cloned_"+edge.mxCell.attrib["target"]
        #mxfile.diagram.mxGraphModel.root.append(elem)
        mxfile.diagram.mxGraphModel.root.insert(mxfile.diagram.mxGraphModel.root.index(edge) + 1, elem)
    return mxfile


## wrap edge into UserObject before duplicating


    """
    duplicated column edges added tags
    Try to build action and add it to the column and read styles to the edges and column duplciated and add visible to 0
    """

if __name__ == "__main__":
    with open("F_SUBSCRIBER_BASE_SEMANTIC_D.drawio") as file:
        xlstr = file.read()

    mxfile = objectify.fromstring(bytes(xlstr,'utf-8'))
    obj = getTargetTableObject(mxfile,"F_SUBSCRIBER_BASE_SEMANTIC_D")
    lsColumns = getTargetColumnsObject(mxfile,obj)
    mxfile = duplicateColumns(mxfile,lsColumns)
    for col in lsColumns:

        mxfile = duplicateEdgesForColumn(mxfile,col)

    et = etree.ElementTree(mxfile)
    et.write("output.drawio",pretty_print=True)
    print(str(["d","ss"]))
    fd="HGR"
    print(fd[:-1])




