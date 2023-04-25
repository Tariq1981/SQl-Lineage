import os
import pydot
from collections import defaultdict

class lineageDiagram:
    def __init__(self,graphName,templateFullPath):
        self.templateFullPath = templateFullPath
        self.graphName = graphName

    def createGraph(self):
        if self.templateFullPath and os.path.exists(self.templateFullPath):
            self.graph = pydot.graph_from_dot_file(self.templateFullPath)
        else:
            self.graph = pydot.Dot(self.graphName,
                              graph_type='digraph',
                              bgcolor='#FFFFFF',
                              fontname='Arial',
                              layout='dot',
                              rankdir='LR')

    def createNode(self,headerColor,table,columnsList):
        if not headerColor:
            headerColor = "#96be5c"
        label = self.__createNodeLabel__(headerColor,table,columnsList)
        tableNode = pydot.Node(table, label=label,color="#FFFFFF",shape="plain")
        self.graph.add_node(tableNode)

    def createEdge(self,edgeColor,srcTable,srcColmn,tgtTable,tgtColumn):
        if not edgeColor:
            edgeColor = "#aeaeae"
        tableEdge = pydot.Edge("{}:{}".format(srcTable,srcColmn),
                               "{}:{}".format(tgtTable,tgtColumn),
                               color=edgeColor)
        self.graph.add_edge(tableEdge)

    def saveGraphAsPNG(self,fullFileNamePath):
        prts=fullFileNamePath.split(".")
        self.graph.write_raw(prts[0]+".dot")
        self.graph.write_svg(prts[0] + ".svg")
        self.graph.write_png(fullFileNamePath)

    def __createNodeLabel__(self,headerColor,table,columnsList):
        label = '<<table border="0" cellborder="1" cellspacing="0" cellpadding="4">' \
                '<th > <td bgcolor="{}"><font face="Arial" color="#FFFFFF">{}</font></td> </th>'.format(headerColor,table)
        for column in columnsList:
            label = label + '<tr> <td bgcolor="#EEEEEE" align="left" port="{}"><font face="Arial">{}</font></td> </tr>'.format(column,column)

        label = label + '</table>>'

        return label
