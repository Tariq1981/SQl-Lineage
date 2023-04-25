from collections import defaultdict
import hashlib
class DrawIOLineageGenerator:
    def __init__(self,
                 templatePath,
                 templateFile,
                 tableStyleName,
                 columnStyleName):
        self.templatePath=templatePath
        self.templateFile=templateFile
        self.tableStyleName=tableStyleName
        self.columnStyleName=columnStyleName
        self.tables=defaultdict(dict)
        self.header="id,name,styletype,parentBox,refs\n"

    def addTable(self,name):
        id = self.__generateId__(name)
        if name not in self.tables:
            self.tables[name] = {"ID":id,"columns":defaultdict(dict)}
    def addColumn(self,tableName,colName,fromTableName,fromColName):
        id = self.__generateId__(tableName+"_"+colName)
        self.tables[tableName]["columns"][colName]["ID"] = id
        if "fromID" not in self.tables[tableName]["columns"][colName]:
            self.tables[tableName]["columns"][colName]["fromID"]=set()
        if fromColName and fromTableName:
            self.tables[tableName]["columns"][colName]["fromID"].add(self.__generateId__(fromTableName+"_"+fromColName))

    def generateCSVDrawIO(self,outputPath,outputFileName):
        fullPath = "{}/{}".format(self.templatePath,self.templateFile)
        with open(fullPath,"r") as fl:
            lines=fl.readlines()
        csvLines = self.__generateCSV__()
        outputFullPath="{}/{}".format(outputPath,outputFileName)
        with open(outputFullPath,'w') as fw:
            fw.writelines(lines)
            fw.writelines(csvLines)

    def __generateCSV__(self):
        rows = []
        rows.append(self.header)
        for tableName in self.tables.keys():
            row = "{},{},{},,\n".format(self.tables[tableName]["ID"],tableName,self.tableStyleName)
            rows.append(row)
            for columnName in self.tables[tableName]['columns'].keys():
                column = self.tables[tableName]['columns'][columnName]
                row = "{},{},{},{},{}\n".format(column['ID'],
                                              columnName,
                                              self.columnStyleName,
                                              self.tables[tableName]["ID"],
                                              '"'+",".join(column['fromID'])+'"')
                rows.append(row)
        return rows

    def __generateId__(self,name):
        result = hashlib.md5(name.encode())
        return result.hexdigest()




if __name__ == "__main__":
    from N2G import yed_diagram

    diagram = yed_diagram()
    diagram.add_node('R1', top_label='Core', bottom_label='ASR1004')
    diagram.add_node('R2', top_label='Edge', bottom_label='MX240')
    diagram.add_link('R1', 'R2', label='DF', src_label='Gi0/1', trgt_label='ge-0/1/2')
    diagram.layout(algo="kk")
    diagram.dump_file(filename="Sample_graph.graphml", folder="./")
