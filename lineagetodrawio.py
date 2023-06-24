from lxml import etree, objectify
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

    def addTable(self,name,columns,x=0,y=0,width=295,headerheight=25):
        id = self.__generateId__(name)
        userobj = objectify.SubElement(self.mxfile.diagram.mxGraphModel.root,"UserObject",
                                       attrib={"id":id,"name":name,"label":name})
        mxcell = objectify.SubElement(userobj,"mxCell",attrib={"style":self.tableStyle,"vertex":"1","parent":"1"})
        objectify.SubElement(mxcell,"mxGeometry",attrib={"x":str(x),"y":str(y),"width":str(width),
                                                         "height":str(headerheight),"as":"geometry"})
        self.__addColumn__(id,name,columns,headerheight)

    def __addColumn__(self,tableId,tablename,columnsList,itemHeight):
        y = itemHeight
        for column in columnsList:
            id = self.__generateId__(tablename+"_"+column)
            userobj=objectify.SubElement(self.mxfile.diagram.mxGraphModel.root, "UserObject",
                                 attrib={"id": id, "name": column,"label":column})
            mxcell = objectify.SubElement(userobj, "mxCell",
                                          attrib={"style": self.columnStyle, "vertex": "1", "parent":tableId})
            objectify.SubElement(mxcell, "mxGeometry", attrib={"y":str(y),"width": "295",
                                                               "height": str(itemHeight), "as": "geometry"})
            y+=25


    def addTag(self,id,tag):
        pass

    def __getColId__(self,tableName,colName):
        predicate = "UserObject[@name = '{}']/mxCell[@vertex='1']/..".format(tableName)
        tblObj = list(self.mxfile.diagram.mxGraphModel.root.iterfind(predicate))
        tblId = tblObj[0].attrib["id"]
        predicate = "UserObject[@name = '{}']/mxCell[@vertex='1'][@parent='{}']/..".format(colName, tblId)
        colObj = list(self.mxfile.diagram.mxGraphModel.root.iterfind(predicate))
        colId = colObj[0].attrib["id"]
        return colId

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

    def __str__(self):
        objectify.deannotate(self.mxfile, cleanup_namespaces=True)
        gg = etree.tostring(self.mxfile,pretty_print=True)
        return gg.decode("utf-8")
    def saveToFile(self,outFilePath,outFileName):
        et = etree.ElementTree(self.mxfile)
        et.write("{}/{}".format(outFilePath,outFileName),pretty_print=True)


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
