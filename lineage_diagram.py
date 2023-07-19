import os
import pydot
from collections import defaultdict

class LineageDiagram:
    def __init__(self,graphName,templateFullPath):
        self.templateFullPath = templateFullPath
        self.graphName = graphName

    def createGraph(self,bgColor='#FFFFFF',fontName='Arial',nodeSep=0.5,rankSep=4):
        if self.templateFullPath and os.path.exists(self.templateFullPath):
            self.graph = pydot.graph_from_dot_file(self.templateFullPath)
        else:
            self.graph = pydot.Dot(self.graphName,
                                   graph_type='digraph',
                                   bgcolor=bgColor,
                                   fontname=fontName,
                                   layout='dot',
                                   rankdir='LR',
                                   nodesep=str(nodeSep),
                                   ranksep=str(rankSep),
                                   pad="0.1",
                                   concentrate="true",
                                   defaultdist="0.1",
                                   maxiter="1000",
                                   newrank="true",
                                   overlap="prism",
                                   pack="true"
                                   )

    def createNode(self,headerColor,table,columnsList):
        label = self.__createNodeLabel__(headerColor,table,columnsList)
        tableNode = pydot.Node(table, label=label,color="#FFFFFF",shape="plain")
        self.graph.add_node(tableNode)

    def createEdge(self,edgeColor,srcTable,srcColmn,tgtTable,tgtColumn):
        tableEdge = pydot.Edge("{}:{}".format(srcTable,srcColmn),
                               "{}:{}".format(tgtTable,tgtColumn),
                               color=edgeColor)
        self.graph.add_edge(tableEdge)

    def saveGraphAsPNG(self,fullFileNamePath):
        prts=fullFileNamePath.split(".")
        self.graph.write_png(fullFileNamePath)

    def saveGraphAsSVG(self,fullFileNamePath):
        prts = fullFileNamePath.split(".")
        self.graph.write_svg(prts[0] + ".svg")

    def saveGraphAsDOT(self,fullFileNamePath):
        prts = fullFileNamePath.split(".")
        self.graph.write_raw(prts[0]+".dot")
    def getJsonGraphviz(self):
        return self.graph.create(format="plain")

    def getNodesPos(self):
        dotG = self.graph.create_dot(prog="dot")
        df = str(dotG, encoding='UTF-8')
        Q_list = pydot.graph_from_dot_data(df)
        Q = Q_list[0]
        node_pos = {}
        for n in self.graph.get_node_list():
            str_n = str(n.obj_dict["name"])
            pydot_node = pydot.Node(str_n).get_name()
            node = Q.get_node(pydot_node)

            if isinstance(node, list):
                node = node[0]
            pos = node.get_pos()[1:-1]  # strip leading and trailing double quotes
            if pos is not None:
                xx, yy = pos.split(",")
                node_pos[str_n] = (float(xx), float(yy))
        return node_pos


    def __createNodeLabel__(self,headerColor,table,columnsList):
        label = '<<table border="0" cellborder="1" cellspacing="0" cellpadding="4">' \
                '<th > <td bgcolor="{}"><font face="Arial" color="#FFFFFF">{}</font></td> </th>'.format(headerColor,table)
        for column in columnsList:
            label = label + '<tr> <td bgcolor="#EEEEEE" align="left" port="{}"><font face="Arial">{}</font></td> </tr>'.format(column,column)
        label = label + '</table>>'
        return label
