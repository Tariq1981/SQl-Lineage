from lxml import etree, objectify
from copy import deepcopy
import hashlib
class LineageToDrawIO:
    def __init__(self,tableStyle,columnStyle,edgeStyle):
        self.tableStyle = tableStyle
        self.columnStyle = columnStyle
        self.edgeStyle = edgeStyle
        self.mxfile = objectify.Element("mxfile")
        self.mxfile.attrib["type"] = "device"
        objectify.SubElement(self.mxfile,"diagram")
        self.mxfile.diagram.attrib["id"]="Page-1"
        self.mxfile.diagram.attrib["name"]="Page-1"
        objectify.SubElement(self.mxfile.diagram,"mxGraphModel")
        self.mxfile.diagram.mxGraphModel.attrib.update({"dx":"1036","dy":"614","grid":"1","gridSize":"10","guides":"1",
                                           "tooltips":"1","connect":"1","arrows":"1","fold":"1","page":"1",
                                           "pageScale":"1","pageWidth":"1100","pageHeight":"850","math":"0",
                                           "shadow":"0"
                                           })
        objectify.SubElement(self.mxfile.diagram.mxGraphModel,"root")
        objectify.SubElement(self.mxfile.diagram.mxGraphModel.root,"mxCell",attrib={"id":"0"})
        objectify.SubElement(self.mxfile.diagram.mxGraphModel.root, "mxCell", attrib={"id": "1","parent":"0"})

    def __generateId__(self,name):
        result = hashlib.md5(name.encode())
        return result.hexdigest()

    def addTable(self,name,columns,x=0,y=0,width=295,headerheight=25,collapsed=False):
        userobj = self.__checkTableExist__(name)
        id = self.__generateId__(name)
        if userobj is None:
            userobj = objectify.SubElement(self.mxfile.diagram.mxGraphModel.root,"UserObject",
                                           attrib={"id":id,"name":name,"label":name})
            if collapsed:
                mxcell = objectify.SubElement(userobj, "mxCell", attrib={"style": self.tableStyle, "vertex": "1",
                                                                         "parent": "1", "collapsed": "1"})
            else:
                mxcell = objectify.SubElement(userobj, "mxCell",
                                              attrib={"style": self.tableStyle, "vertex": "1", "parent": "1"})

            objectify.SubElement(mxcell,"mxGeometry",attrib={"x":str(x),"y":str(y),"width":str(width),
                                                             "height":str(headerheight),"as":"geometry"})
        self.__addColumn__(id,name,columns,headerheight,width)


    def __addColumn__(self,tableId,tablename,columnsList,itemHeight,itemWidth):
        y = itemHeight
        for column in columnsList:
            if self.__getColId__(tablename,column):
                continue
            id = self.__generateId__(tablename+"_"+column)
            userobj=objectify.SubElement(self.mxfile.diagram.mxGraphModel.root, "UserObject",
                                 attrib={"id": id, "name": column,"label":column})
            mxcell = objectify.SubElement(userobj, "mxCell",
                                          attrib={"style": self.columnStyle, "vertex": "1", "parent":tableId})
            objectify.SubElement(mxcell, "mxGeometry", attrib={"y":str(y),"width": str(itemWidth),
                                                               "height": str(itemHeight), "as": "geometry"})
            y+=itemHeight

    def getEdgesToTargetList(self,targetId):
        predicate = "UserObject[@id = '{}']".format(id)
        tblObj = list(self.mxfile.diagram.mxGraphModel.root.iterfind(predicate))

    def __checkTableExist__(self,tableName):
        id = self.__generateId__(tableName)
        predicate = "UserObject[@id = '{}']".format(id)
        tblObj = list(self.mxfile.diagram.mxGraphModel.root.iterfind(predicate))
        if len(tblObj) > 0:
            return tblObj[0]
        return None

    def __getColId__(self,tableName,colName):
        predicate = "UserObject[@name = '{}']/mxCell[@vertex='1']/..".format(tableName)
        tblObj = list(self.mxfile.diagram.mxGraphModel.root.iterfind(predicate))
        if len(tblObj) > 0:
            tblId = tblObj[0].attrib["id"]
            predicate = "UserObject[@name = '{}']/mxCell[@vertex='1'][@parent='{}']/..".format(colName, tblId)
            colObj = list(self.mxfile.diagram.mxGraphModel.root.iterfind(predicate))
            if len(colObj) > 0:
                colId = colObj[0].attrib["id"]
                return colId
        return None

    def addEdge(self,srcTblName,srcColName,tgtTblName,tgtColName):
        srcColId = self.__getColId__(srcTblName,srcColName)
        tgtColId = self.__getColId__(tgtTblName,tgtColName)
        id = self.__generateId__("{}_{}_{}_{}".format(srcTblName,srcColName,tgtTblName,tgtColName))
        userobj = objectify.SubElement(self.mxfile.diagram.mxGraphModel.root, "UserObject",
                                       attrib={"id": id,"label": ""})
        mxcell = objectify.SubElement(userobj, "mxCell",
                                      attrib={"style": self.edgeStyle, "edge": "1", "parent": "1",
                                              "source":srcColId,"target":tgtColId
                                              })
        geom = objectify.SubElement(mxcell, "mxGeometry",
                             attrib={"relative":"1", "as": "geometry"})
        objectify.SubElement(geom,"mxPoint",attrib={"as":"offset"})

    def moveEdgesToBack(self):
        predicate = "UserObject/mxCell[@edge='1']/.."
        edges = list(self.mxfile.diagram.mxGraphModel.root.iterfind(predicate))
        for edge in edges:
            self.mxfile.diagram.mxGraphModel.root.insert(0,edge)

    def __str__(self):
        objectify.deannotate(self.mxfile, cleanup_namespaces=True)
        gg = etree.tostring(self.mxfile,pretty_print=True)
        return gg.decode("utf-8")
    def saveToFile(self,outFilePath,outFileName):
        et = etree.ElementTree(self.mxfile)
        et.write("{}/{}".format(outFilePath,outFileName),pretty_print=True)

    def __getObjName__(self,columnObject):
        if "name" in columnObject.attrib:
            return columnObject.attrib["name"]
        else:
            return columnObject.attrib["label"]

    def __setStyle__(self,columnObj, stylesDict, removeEdge=True):
        style = columnObj.mxCell.attrib["style"]
        stList = style[:-1].split(";")
        stDict = {}
        for st in stList:
            keyval = st.split("=")
            if len(keyval) < 2:
                stDict[keyval[0]] = ""
            else:
                stDict[keyval[0]] = keyval[1]
        for key in stylesDict.keys():
            stDict[key] = stylesDict[key]
        strStyle = ""
        for key in stDict.keys():
            if key == "noEdgeStyle" and removeEdge:
                continue
            elif key == "edgeStyle" and removeEdge:
                strStyle += "{}={};".format(key, "entityRelationEdgeStyle")
                continue
            elif len(stDict[key]) == 0:
                strStyle += "{};".format(key)
            else:
                strStyle += "{}={};".format(key, stDict[key])
        columnObj.mxCell.attrib["style"] = strStyle
        return columnObj

    def __createAction__(self,destColName,linId ,normStyles, selStyles):
        """
        Get list of tags and create te selct and normal action
        """

        show_norm = '{{"show":{{"tags":["norm_{}_{}"]}}}}'.format(linId,destColName)
        show_sel = '{{"show":{{"tags":["sel_{}_{}"]}}}}'.format(linId,destColName)
        hide_norm = '{{"hide":{{"tags":["norm_{}_{}"]}}}}'.format(linId,destColName)
        hide_sel = '{{"hide":{{"tags":["sel_{}_{}"]}}}}'.format(linId,destColName)

        show_norm_arrows = '{{"show":{{"tags":["norm_{}_arrows"]}}}}'.format(linId)

        hide_norm_arrows = '{{"hide":{{"tags":["norm_{}_arrows"]}}}}'.format(linId)
        hide_sel_arrows = '{{"hide":{{"tags":["sel_{}_arrows"]}}}}'.format(linId)

        style_norm = []
        for key in normStyles.keys():
            temp = '{{"style":{{"tags":["src_{}_{}"],"key":"{}","value":"{}"}}}}'.format(linId,destColName, key, normStyles[key])
            style_norm.append(temp)

        final_norm_style = ",".join(style_norm)
        style_sel = []
        for key in selStyles.keys():
            temp = '{{"style":{{"tags":["src_{}_{}"],"key":"{}","value":"{}"}}}}'.format(linId,destColName, key, selStyles[key])
            style_sel.append(temp)
        final_sel_style = ",".join(style_sel)
        action_show_norm = 'data:action/json,{{"actions":[{},{},{},{},{}]}}'.format(show_norm, show_norm_arrows,
                                                                                    hide_sel, hide_sel_arrows,
                                                                                    final_norm_style)
        action_show_sel = 'data:action/json,{{"actions":[{},{},{},{}]}}'.format(show_sel, hide_norm, hide_norm_arrows,
                                                                                final_sel_style)
        return (action_show_norm, action_show_sel)

    def __duplicateColumns__(self,linId, columnsList):
        for col in columnsList:
            elem = deepcopy(col)
            elem.attrib["id"] = "cloned_"+linId+"_" + elem.attrib["id"]

            elem.attrib["tags"] = "sel_"+linId+"_"+ self.__getObjName__(elem)
            col.attrib["tags"] = "norm_" +linId+"_"+ self.__getObjName__(elem)

            elem.mxCell.attrib['visible'] = "0"
            col.mxCell.attrib['visible'] = "1"

            elem = self.__setStyle__(elem, {"gradientColor": "#ffa500"})

            (normAction, selAction) = self.__createAction__(self.__getObjName__(elem),linId, {"gradientColor": "#b3b3b3"},
                                                            {"gradientColor": "#ffa500"})
            elem.attrib["link"] = normAction
            col.attrib["link"] = selAction
            # par = col.getparent()
            self.mxfile.diagram.mxGraphModel.root.insert(self.mxfile.diagram.mxGraphModel.root.index(col) + 1, elem)
            # mxfile.diagram.mxGraphModel.root.append(elem)
        return self.mxfile

    def __getEdgeListTargetColumn__(self, columnObject):
        colId = columnObject.attrib["id"]
        predicate = "UserObject/mxCell[@edge='1'][@target='{}']/..".format(colId)
        edges = self.mxfile.diagram.mxGraphModel.root.iterfind(predicate)
        return edges

    def __addTagToSourceColumn__(self, srcColumnId,linId ,destColumnObject):
        predicate = "UserObject[@id = '{}']/mxCell[@vertex='1']/..".format(srcColumnId)
        srcCol = list(self.mxfile.diagram.mxGraphModel.root.iterfind(predicate))
        srcCol = srcCol[0]
        if "tags" in srcCol.attrib:
            tags = srcCol.attrib["tags"]
        else:
            tags = ""
        tgls = tags.split(" ")
        tgls.append("src_" + linId+"_" + self.__getObjName__(destColumnObject))
        if len(tgls[0]) == 0:
            tgls.pop(0)
        srcCol.attrib["tags"] = " ".join(tgls)

    def __duplicateEdgesForColumn__(self, columnObject,linId, removeEdge=True,selectedStrokCol="#f51919",selectedStrokWidth=3):
        edges = list(self.__getEdgeListTargetColumn__(columnObject))
        for edge in edges:
            srcId = edge.mxCell.attrib["source"]
            self.__addTagToSourceColumn__(srcId,linId ,columnObject)
            elem = deepcopy(edge)
            elem.attrib["id"] = "cloned_" +linId+"_"+ elem.attrib["id"]
            elem.attrib["tags"] = "sel_"+linId+"_" + self.__getObjName__(columnObject) + " sel_{}_arrows".format(linId)
            edge.attrib["tags"] = "norm_" + linId+"_" +self.__getObjName__(columnObject) + " norm_{}_arrows".format(linId)
            elem = self.__setStyle__(elem, {"strokeColor": selectedStrokCol,"strokeWidth":str(selectedStrokWidth)},
                                     removeEdge=removeEdge)
            edge = self.__setStyle__(edge, {"strokeColor": "#000000"}, removeEdge=removeEdge)
            elem.mxCell.attrib['visible'] = "0"
            edge.mxCell.attrib['visible'] = "1"
            elem.mxCell.attrib["target"] = "cloned_" + linId + "_" + edge.mxCell.attrib["target"]
            # mxfile.diagram.mxGraphModel.root.append(elem)
            self.mxfile.diagram.mxGraphModel.root.insert(self.mxfile.diagram.mxGraphModel.root.index(edge) + 1, elem)
        return self.mxfile

    def __getTargetColumnsObject__(self, targetTableObj):
        id = targetTableObj.attrib["id"]
        predicate = "UserObject/mxCell[@vertex='1'][@parent='{}']/..".format(id)
        cols = self.mxfile.diagram.mxGraphModel.root.iterfind(predicate)
        return list(cols)

    def __getTargetTableObject__(self,targetName):
        for obj in self.mxfile.diagram.mxGraphModel.root.iterchildren(tag='UserObject'):
            if "vertex" in obj.mxCell.attrib and obj.mxCell.attrib['vertex'] == "1":
                if "name" in obj.attrib:
                    if obj.attrib['name'] == targetName:
                        return obj
                else:
                    if obj.attrib['label'] == targetName:
                        return obj
        return None


    def addInteractionToDiagram(self,linId,targetName,selStrokCol="#f51919",strokWidth=3):
        obj = self.__getTargetTableObject__(targetName)
        lsColumns = self.__getTargetColumnsObject__(obj)
        self.mxfile = self.__duplicateColumns__(linId,lsColumns)
        for col in lsColumns:
            self.mxfile = self.__duplicateEdgesForColumn__(col,linId, removeEdge=False,selectedStrokCol=selStrokCol,
                                                           selectedStrokWidth=strokWidth)
    def moveNode(self,tableName,xCoord,yCoord):
        userObj = self.__checkTableExist__(tableName)
        if userObj is not None:
            userObj.mxCell.mxGeometry.attrib["x"] = str(xCoord)
            userObj.mxCell.mxGeometry.attrib["y"] = str(yCoord)



if __name__ == "__main__":
    rr= LineageToDrawIO("shape=swimlane;fontStyle=0;childLayout=stackLayout;horizontal=1;startSize=26;horizontalStack=0;resizeParent=1;resizeParentMax=0;resizeLast=0;collapsible=1;marginBottom=0;align=center;fontSize=14;fillColor=#60a917;strokeColor=#2D7600;fontColor=#ffffff;",
                        "text;spacingLeft=4;spacingRight=4;overflow=hidden;rotatable=0;points=[[0,0.5],[1,0.5]];portConstraint=eastwest;fontSize=12;whiteSpace=wrap;html=1;fillColor=#f5f5f5;fontColor=#333333;strokeColor=#666666;gradientColor=#b3b3b3;",
                        "edgeStyle=orthogonalEdgeStyle;orthogonalLoop=1;jettySize=auto;html=1;curved=1;strokeWidth=2;flowAnimation=0;gradientColor=#b3b3b3;strokeColor=#000000;entryX=0;entryY=0.5;entryDx=0;entryDy=0;noEdgeStyle=1;orthogonal=1;")
    rr.addTable("Table1",["col11","col12"])
    rr.addTable("Table2", ["col21", "col22"])
    rr.addTable("Dest",["dest1","dest2"])

    rr.addEdge("Table1","col11","Dest","dest1")
    rr.addEdge("Table2", "col21", "Dest", "dest1")

    rr.addEdge("Table1", "col12", "Dest", "dest2")
    rr.addEdge("Table2", "col22", "Dest", "dest2")

    print(str(rr))
