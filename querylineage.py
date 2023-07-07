import os
from collections import defaultdict
import configparser
from itertools import filterfalse
from sqlglot import parse_one
import sqlglot.expressions as exp
from simple_ddl_parser import DDLParser
from sqllineage.runner import LineageRunner
from sqllineage.utils.constant import LineageLevel
from lineage_diagram import LineageDiagram
import sqlparse
from sqlparse.sql import Statement,TokenList,Token,Identifier
from sqlparse import tokens as T

""" 
Queries in the path with name as target table + .sql
"""
class QueryLineage:
    CONFIG_FILE_NAME = "lineage.config"
    DEFAULT_TABLE_HEADER = "#96be5c"
    def __init__(self,sqlPath,DDLPath,templateFullPath,templateFileName,configPath):
        self.sqlPath=sqlPath
        self.DDLPath = DDLPath
        self.templateFullPath = templateFullPath
        self.templateFileName = templateFileName
        self.configPath = configPath
        self.tablesSet = defaultdict(set)
        self.relationsSet = defaultdict(set)
        self.DBTableLookup = defaultdict(lambda:'DEFAULT')
        self.config = configparser.SafeConfigParser()
        self.readConfigFile()
        self.__parseDDL__()

    def __parseDDL__(self):
        files = os.listdir(self.DDLPath)
        for file in files:
            fileName = "{}/{}".format(self.DDLPath,file)
            if os.path.isfile(fileName):
                with open(fileName,"r") as f:
                    lines = f.readlines()
                ddl = "".join(lines)
                result = DDLParser(ddl).run(output_mode="bigquery")
                for table in result:
                    for column in table['columns']:
                        self.tablesSet[table['table_name']].add(column['name'])


    def getFullSQLLineage(self,entrytableName):
        self.entryTableName = entrytableName
        ls = [entrytableName.upper()]
        vistied = set()
        while len(ls) > 0:
            tableName = ls.pop()
            if tableName in vistied:
                continue
            tablesRelations = self.__getLineage__(tableName)
            vistied.add(tableName)
            if not(tablesRelations):
                continue
            tables = tablesRelations["tables"]
            relations = tablesRelations["relations"]
            ls.extend(tables.keys())
            self.__updateTablesRelations__(tables,relations)

    def __removePartitionBy__(self,sql):
        stmt = sqlparse.parse(sql)
        tokens: TokenList = stmt[0].tokens
        ls = []
        isPartition = False
        isBy = False
        c1: Token = None
        c2: Token = None
        c3: Token = None
        for tok in tokens:
            c: Token = tok
            if c.is_keyword and c.value.upper() == "PARTITION":
                isPartition = True
                c1 = c
                continue
            elif c.is_keyword and c.value.upper() == "BY" and isPartition:
                isBy = True
                c2 = c
                continue
            elif isinstance(c, Identifier) and isBy:
                c3 = c
                isPartition = False
                isBy = False
                ls.extend(map(str, c.tokens[1:]))
                continue
            ls.append(str(c))
        return "".join(ls)

    def __updateTablesRelations__(self,tables,relations):
        for table in tables.keys():
            if table not in self.tablesSet:
                self.tablesSet[table].update(tables[table])

        for relation in relations.keys():
            if relation not in self.relationsSet:
                self.relationsSet[relation] = relations[relation]

    def createGraph(self):
        self.diagram = LineageDiagram(self.entryTableName, "{}/{}".format(self.templateFullPath, self.templateFileName))
        self.diagram.createGraph()
        for table in self.tablesSet.keys():
            db = self.DBTableLookup[table]
            tableHeaderColor = self.__getConfigItem__('DB_COLOR',db)
            if not tableHeaderColor:
                tableHeaderColor = self.DEFAULT_TABLE_HEADER
            self.diagram.createNode(tableHeaderColor,table,list(self.tablesSet[table]))

        for relation in self.relationsSet.keys():
            tgtTable = relation[0]
            tgtColumn = relation[1]
            for src in self.relationsSet[relation]:
                srcTable = src[0]
                srcColumn = src[1]
                edgColor = self.__getConfigItem__("EDGE_COLOR","{}_{}".format(tgtTable,srcTable))
                self.diagram.createEdge(edgColor,srcTable,srcColumn,tgtTable,tgtColumn)


    def writeDiagramToPNG(self,fullPath):
        self.diagram.saveGraphAsPNG(fullPath)

    def readConfigFile(self):
        self.config.read("{}/{}".format(self.configPath,self.CONFIG_FILE_NAME))
    def __getConfigItem__(self,section,item):
        if section in self.config and item in self.config[section]:
            return self.config[section][item]
        else:
            return None

    def __getLineage__(self, entrytableName):
        raw_sql = self.readSql(entrytableName)
        if not (raw_sql):
            return None
        statements = raw_sql.split(";")
        for stmt in statements:
            if len(stmt)==0:
                continue
            prepSql = self.__removePartitionBy__(stmt)
            finalPrepSql = self.__replaceStarInScript__(prepSql,self.tablesSet)
            result = LineageRunner(str(finalPrepSql), verbose=True)
            col_lin = result.get_column_lineage(exclude_subquery=True)
            sources = result.source_tables
            targets = result.target_tables
            tablesRelations = self.__getTablesColumnsRelations__(col_lin,sources,targets)
            tables = tablesRelations["tables"]
            relations = tablesRelations["relations"]
            self.__updateTablesRelations__(tables, relations)

    """
    def getLineage(self,entrytableName):
        raw_sql = self.readSql(entrytableName)
        if not(raw_sql):
            return None
        result = LineageRunner(raw_sql, verbose=True)
        columns_relations = result.to_cytoscape(LineageLevel.COLUMN)
        return self.__getTablesColumnsRelations__(columns_relations)
    """

    def __replaceStarInQuery__(self,col: exp.Column, ddlList):
        parent_sel = col.parent_select
        fromJoin = list(parent_sel.find_all(exp.From, exp.Join))
        ls = []
        for f in fromJoin:
            if f.parent_select == parent_sel:
                if isinstance(f, exp.From) and isinstance(f.expressions[0], exp.Table):
                    ls.extend(ddlList[f.expressions[0].this.alias_or_name])
                elif isinstance(f, exp.Join) and isinstance(f.this, exp.Join):
                    ls.extend(ddlList[f.this.alias_or_name])
                else:
                    fromObj = None
                    if isinstance(f, exp.From):
                        fromObj = f.expressions[0]
                    else:
                        fromObj = f.this
                    if hasattr(col, "table") and len(col.table) > 0:
                        if col.table == fromObj.alias:
                            sels = list(f.find_all(exp.Select))
                            for coli in sels[0].selects:
                                if isinstance(coli.this, exp.Star):
                                    cols = self.__replaceStarInQuery__(coli, ddlList)
                                    ls.extend(cols)
                                else:
                                    ls.append(coli.alias_or_name)
                            break
                    else:
                        sels = list(f.find_all(exp.Select))
                        for coli in sels[0].selects:
                            if isinstance(coli.this, exp.Star):
                                cols = self.__replaceStarInQuery__(coli, ddlList)
                                ls.extend(cols)
                            else:
                                ls.append(coli.alias_or_name)

        return ls


    def __replaceStarInScript__(self,sqlState, ddlList):
        sqlObj = parse_one(sqlState, "bigquery")
        sels = list(sqlObj.find_all(exp.Select))

        for sel in sels:
            ls = []
            cols = sel.selects
            for ind in range(0, len(cols)):
                col = cols[ind]
                if isinstance(col.this, exp.Star):
                    strSql = self.__replaceStarInQuery__(col, ddlList)
                    finalList = map(lambda X: ".".join([col.table, X]), strSql)
                    if 'except' in col.this.args and col.this.args['except']:
                        def checkExist(colName):
                            for clname in col.this.args['except']:
                                if clname.alias_or_name.upper() == colName.upper():
                                    return True
                            return False

                        finalList = list(filterfalse(checkExist, finalList))

                    ls.append((col, list(finalList)))
                elif isinstance(col, exp.Star):
                    strSql = self.__replaceStarInQuery__(col, ddlList)
                    finalList = strSql
                    if 'except' in col.args and col.args['except']:
                        def checkExist(colName):
                            for clname in col.args['except']:
                                if clname.alias_or_name.upper() == colName.upper():
                                    return True
                            return False

                        finalList = list(filterfalse(checkExist, finalList))

                    ls.append((col, list(finalList)))

            for item in ls:
                col, coList = item
                cols.remove(col)
            for item in ls:
                ind, coList = item
                for cl in coList:
                    cc: exp.Column = exp.to_column(cl)
                    cc.parent = sel
                    cols.append(cc)

        return sqlObj

    def __getTablesColumnsRelations__(self, colLineages,sourcetables,targetTables):
        """
        We need to have DDL for the tables and build dictionary key column name and value list of tables
        Then if we have subquery try to find the tablename for the column in hand
        
        :param colLineages:
        :param sourcetables:
        :param targetTables:
        :return:
        """
        tables = defaultdict(set)
        relations = defaultdict(set)
        for table in sourcetables:
            tableName = table.raw_name.upper()
            tables[tableName]=set()
            self.DBTableLookup[tableName] = table.schema.raw_name.upper()

        for table in targetTables:
            tableName = table.raw_name.upper()
            tables[tableName]=set()
            self.DBTableLookup[tableName] = table.schema.raw_name.upper()

        for relation in colLineages:
            srcColObj = relation[0]
            tgtMColObj = relation[-1]

            srcColumn = srcColObj.raw_name.upper()
            srcTable = srcColObj.parent_candidates[0].raw_name.upper()

            tgtColumn = tgtMColObj.raw_name.upper()
            tgtTable = tgtMColObj.parent_candidates[0].raw_name.upper()

            relations[(tgtTable, tgtColumn)].add((srcTable, srcColumn))
            tables[srcTable].add(srcColumn)
            tables[tgtTable].add(tgtColumn)

        return {'tables': tables, 'relations': relations}

    """
    def __getTablesColumnsRelations__(self,lineageResults):
        tables = defaultdict(set)
        relations = defaultdict(set)
        for item in lineageResults:
            if "type" in item["data"] and item["data"]["type"] == "Column":
                fullTableName = item["data"]['parent'].upper()
                tableNameparts = fullTableName.split(".")
                tableName = tableNameparts[-1]
                if len(tableNameparts) >= 2:
                    self.DBTableLookup[tableName] = tableNameparts[-2]

                fullColumnName = item["data"]['id'].upper()
                columnNameparts = fullColumnName.split(".")
                columnName = columnNameparts[-1]
                tables[tableName].add(columnName)
            elif "source" in item["data"] and "target" in item["data"]:
                srcMap = item["data"]["source"].upper()
                tgtMap = item["data"]["target"].upper()

                srcParts = srcMap.split(".")
                srcColumn = srcParts[-1]
                srcTable = srcParts[-2]

                tgtParts = tgtMap.split(".")
                tgtColumn = tgtParts[-1]
                tgtTable = tgtParts[-2]

                relations[(tgtTable, tgtColumn)].add((srcTable, srcColumn))

        return {'tables': tables, 'relations': relations}
    """
    def readSql(self,entrytableName):
        fullPath = "{}/{}.sql".format(self.sqlPath,entrytableName)
        if not(os.path.exists(fullPath)):
            return None
        with open(fullPath,'r') as file:
            lines = file.readlines()
        return "".join(lines)


if __name__ == "__main__":
    ln=QueryLineage("./","./DDL",None,"./","./")
    ln.getFullSQLLineage("Tab3")
    ln.createGraph()
    ln.writeDiagramToPNG("Tab3.png")
    """
    Queries in the insert must have aliases same as column names of the target table
    
    ##### REsolve if more than one subquery with same name should each one to be unique
    """

