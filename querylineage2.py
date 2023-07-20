import os
from collections import defaultdict
import time
import configparser
import re
import sqlparse
from sqlparse import tokens as T
from sqlparse.sql import TokenList,Token,Identifier,Function
from sqlparse.tokens import Keyword
from simple_ddl_parser import DDLParser
from sqlglot import parse_one
import sqlglot.expressions as exp
from itertools import filterfalse
from lineage_diagram import LineageDiagram
from drawio_gen import DrawIOLineageGenerator
from lineagetodrawio import LineageToDrawIO
from sqllineage.runner import LineageRunner
from sqllineage.utils.constant import LineageLevel


""" 
Queries in the path with name as target table + .sql
"""


class QueryLineageAnalysis:
    DEFAULT_TABLE_HEADER = "#96be5c"
    DEFAULT_EDGE = "#aeaeae"

    def __init__(self, sqlPath, DDLPath, defaultDB="",isDebug=False,debugPath="./"):
        self.sqlPath = sqlPath
        self.DDLPath = DDLPath
        self.defaultDB = defaultDB
        self.isDebug = isDebug
        self.debugPath = debugPath
        self.tablesSet = defaultdict(list)
        self.tablesSetSearch = defaultdict(set)
        self.relationsSet = defaultdict(set)
        self.relationsSetNew = defaultdict(list)
        self.usedTables=set()
        self.usedTablesFiltered = set()
        self.DBTableLookup = defaultdict(lambda: 'DEFAULT')
        self.keywordsList=set(["CREATE","INSERT"])
        self.varNames = set()
        self.linTablesList = []
        self.currentSrcTables = set()
        self.totalTgtTables = set()
        self.pivotColumn = defaultdict()
        self.config = configparser.SafeConfigParser()
        self.__parseDDL__()

    def __parseDDL__(self):
        if not self.DDLPath:
            return
        files = os.listdir(self.DDLPath)
        for file in files:
            fileName = "{}/{}".format(self.DDLPath, file)
            if os.path.isfile(fileName):
                with open(fileName, "r") as f:
                    lines = f.readlines()
                ddl = "".join(lines)
                result = DDLParser(ddl).run(output_mode="bigquery")
                for table in result:
                    for column in table['columns']:
                        tableName = table['table_name'].replace("`","").upper()
                        self.tablesSetSearch[tableName].add(column['name'].upper())
                        self.tablesSet[tableName].append(column['name'].upper())


    def createGraphviz(self,entryTableName,templateFullPath,templateFileName,
                       bgColor="#FFFFFF",fontName="Arial",nodeSep=0.5,rankSep=5,headerColor="#96be5c",
                       edgStrokColor="#aeaeae",useFiltered=False):
        if templateFullPath and len(templateFullPath) > 0 and templateFileName and len(templateFileName) > 0:
            self.diagram = LineageDiagram(entryTableName, "{}/{}".format(templateFullPath, templateFileName))
        else:
            self.diagram = LineageDiagram(entryTableName, None)
        self.diagram.createGraph(bgColor,fontName,nodeSep,rankSep)
        usedT = self.usedTables
        if useFiltered and len(self.usedTablesFiltered) > 0:
            usedT = self.usedTablesFiltered

        tempRelations = self.relationsSet
        if useFiltered and len(self.relationsSetNew.keys()):
            tempRelations = self.relationsSetNew

        dictColumns = defaultdict(set)
        for relation in tempRelations.keys():
            tgtTable = relation[0]
            tgtColumn = relation[1]
            dictColumns[tgtTable].add(tgtColumn)
            for src in tempRelations[relation]:
                srcTable = src[0]
                srcColumn = src[1]
                dictColumns[srcTable].add(srcColumn)
        for tableName in dictColumns.keys():
            db = self.DBTableLookup[tableName]
            tableHeaderColor = headerColor
            if not tableHeaderColor:
                tableHeaderColor = self.DEFAULT_TABLE_HEADER
            self.diagram.createNode(tableHeaderColor, tableName, sorted(list(dictColumns[tableName])))

        for relation in tempRelations.keys():
            tgtTable = relation[0]
            tgtColumn = relation[1]
            for src in tempRelations[relation]:
                srcTable = src[0]
                srcColumn = src[1]
                if edgStrokColor and len(edgStrokColor) > 0:
                    edgColor = edgStrokColor
                else:
                    edgColor = self.DEFAULT_EDGE
                self.diagram.createEdge(edgColor,srcTable,srcColumn,tgtTable,tgtColumn)


    def writeGraphvizToPNG(self,fullPath):
        self.diagram.saveGraphAsPNG(fullPath)
        jsonStr = self.diagram.getJsonGraphviz()
        with open("jsonFile.json","w") as f:
            f.write(jsonStr.decode("utf-8"))


    def generateDrawIOXMLLayout(self,
                                targetTableName,
                                tableStyle,
                                columnStyle,
                                edgeStyle,
                                outputPath,
                                outputFileName,
                                srcfactorSpace=2,
                                srcTgtSpaceFactor=4,
                                distinceSrcTgt=200,
                                itemHieght=25,
                                collapsed=False,
                                isInteractive = True,
                                strokeColor = "#f51919",
                                useFiltered=False):
        """Set x and y based on if it is source or target sources on the let and target on the right
        updateteh xml creation to accept x,y,width and height
        """
        tempRelations = self.relationsSet
        if useFiltered and len(self.relationsSetNew.keys()) > 0:
            tempRelations = self.relationsSetNew
        lin = LineageToDrawIO(tableStyle, columnStyle, edgeStyle)
        totalColumns = 0
        dictColumns = defaultdict(set)
        for relation in tempRelations.keys():
            tgtTable = relation[0]
            tgtColumn = relation[1]
            dictColumns[tgtTable].add(tgtColumn)
            totalColumns+=1
            for src in tempRelations[relation]:
                srcTable = src[0]
                srcColumn = src[1]
                dictColumns[srcTable].add(srcColumn)
                totalColumns += 1

        totalColumns+=len(dictColumns.keys())  ## number of tables as headers
        previousSrcTableEndX = 0
        previousSrcTableEndY = 0
        for tableName in dictColumns.keys():
            if tableName == targetTableName:
                srcColumns = totalColumns - len(dictColumns[tableName])-1
                intial_y = int(srcColumns/4) *itemHieght
                y = intial_y - int(((len(dictColumns[tableName])+1)*itemHieght) /4)
                lin.addTable(tableName, list(sorted(dictColumns[tableName])),
                             distinceSrcTgt*srcTgtSpaceFactor, y,collapsed=collapsed)
            else:
                lin.addTable(tableName, list(sorted(dictColumns[tableName])),
                             previousSrcTableEndX,previousSrcTableEndY,collapsed=collapsed)
                previousSrcTableEndY+= ((srcfactorSpace+len(dictColumns[tableName])) * itemHieght)

        for relation in tempRelations.keys():
            tgtTable = relation[0]
            tgtColumn = relation[1]
            for src in tempRelations[relation]:
                srcTable = src[0]
                srcColumn = src[1]
                lin.addEdge(srcTable, srcColumn, tgtTable, tgtColumn)

        if isInteractive:
            lin.addInteractionToDiagram(targetTableName,strokeColor)
        lin.saveToFile(outputPath, outputFileName)

    def generateDrawIOCSV(self,templatePath,templateFileName,
                          tableStyleName,columnStyleName,
                          outputPath,outputFileName,
                          useFiltered=False):
        tempRelations = self.relationsSet
        if useFiltered and len(self.relationsSetNew.keys()) > 0:
            tempRelations = self.relationsSetNew

        drawgen = DrawIOLineageGenerator(templatePath,templateFileName,tableStyleName,columnStyleName)
        for relation in tempRelations.keys():
            tgtTable = relation[0]
            tgtColumn = relation[1]
            drawgen.addTable(tgtTable)
            for src in tempRelations[relation]:
                srcTable = src[0]
                srcColumn = src[1]
                drawgen.addTable(srcTable)
                drawgen.addColumn(srcTable,srcColumn,tgtTable,tgtColumn)
                drawgen.addColumn(tgtTable,tgtColumn,None,None)
        drawgen.generateCSVDrawIO(outputPath,outputFileName)

    def createfilteredRelations(self,targetTable,dbListExclude):
        keys = list(self.relationsSet)
        self.usedTablesFiltered.add(targetTable)
        for relation in keys:
            tgtTable = relation[0]
            if tgtTable == targetTable:
                ListPairs = [self.relationsSet[relation]]
                visisted = set()
                ls = set()
                while len(ListPairs) > 0:
                    currentListPairs = ListPairs.pop()
                    for srcPair in currentListPairs:
                        if srcPair in visisted:
                            continue
                        visisted.add(srcPair)
                        if self.DBTableLookup[srcPair[0]] in dbListExclude or srcPair[0] == targetTable:
                            ListPairs.append(self.relationsSet[srcPair])
                        else:
                            ls.add(srcPair)
                            self.usedTablesFiltered.add(srcPair[0])
                self.relationsSetNew[relation].extend(ls)

    def __isStmtOK__(self,stmt):
        pp=sqlparse.parse(stmt)
        ls = []
        ls.extend(pp[0])
        while len(ls) > 0:
            ff:Token = ls.pop(0)
            vals = ff.value.upper().split(" ")
            val=vals[0]
            if ff.ttype in (T.Keyword.DML, T.Keyword.DDL) and val in self.keywordsList:
                return True
            if hasattr(ff,"tokens"):
                ls.extend(ff.tokens)
        return False

    def __isDeclareAddVarName__(self,stmt):
        stmt=sqlparse.parse(stmt.strip())
        isDeclare= False
        if len(stmt) > 0 and len(stmt[0].tokens) > 0 and stmt[0].tokens[0].value.upper() == "DECLARE":
            isDeclare = True
            for tok in stmt[0].tokens[1:]:
                if isDeclare and isinstance(tok,Identifier):
                    self.varNames.add(tok.value.upper())
                    break
        return isDeclare


    def getLineage(self, entrytableName,verbose=False):
        raw_sql = self.__readSql__(entrytableName)
        if not (raw_sql):
            return False
        statements = raw_sql.split(";")
        if verbose:
            print("Number of detected Statements: {}".format(len(statements)))
        stmtIndex = 0
        self.currentSrcTables.clear()
        for stmt in statements:
            startTime = time.time()
            stmt = stmt.strip()
            self.__isDeclareAddVarName__(stmt)
            if len(stmt) == 0 or not self.__isStmtOK__(stmt):
                if verbose:
                    stm = stmt.split(" ")
                    print("Statement {} starts with '{}' will be escaped".format(stmtIndex + 1,stm[0]))
                    print("Statements {}/{} has been parsed".format(stmtIndex + 1, len(statements)))
                    print("---------------------------------")
                stmtIndex+=1
                continue

            sql0 = self.__removeComments__(stmt)
            sql0 = self.__removePartitionBy2__(sql0)
            #sql0 = sql0.replace("`", " ")
            #sql0 = sql0.replace("-", "_")
            sql0 = self.__convertCreateSelectToSubuery__(sql0)
            sfg = self.__convertCTEtoSubqueries__(sql0)
            lsff = self.__replaceStarInScript__(sfg, self.tablesSet)
            lin = self.__getSQLLineage__(lsff, self.tablesSet)
            if self.isDebug:
                if len(lin) == 0:
                    print("Query no ineage!!!!: "+lsff)
                else:
                    with open('{}/lin_{}.txt'.format(self.debugPath,lin[0]['TargetTable']),'w') as ff:
                        for l in lin:
                            ff.write(str(l))
                            ff.write('\n')
                    with open('{}/query_{}.txt'.format(self.debugPath,lin[0]['TargetTable']),'w') as ff:
                        ff.write(str(lsff))


            tablesRelations = self.__getTablesColumnsRelations__(lin)
            tables = tablesRelations["tables"]
            relations = tablesRelations["relations"]
            self.__updateTablesRelations__(tables, relations)
            if verbose:
                print("Statement {} parsed in {} seconds".format(stmtIndex+1,(time.time() - startTime)))
                print("Statements {}/{} has been parsed".format(stmtIndex+1,len(statements)))
                print("---------------------------------")
            stmtIndex+=1
        return True

    def __updateTablesRelations__(self,tables,relations):
        for table in tables.keys():
            if table not in self.tablesSet:
                self.tablesSet[table].extend(tables[table])
                self.tablesSetSearch[table].update(tables[table])

        for relation in relations.keys():
            if relation not in self.relationsSet:
                self.relationsSet[relation] = relations[relation]
                tgtTable =relation[0]
                tgtColumn = relation[1]
                self.totalTgtTables.add(tgtTable)
                if tgtColumn not in self.tablesSetSearch[tgtTable]:
                    self.tablesSet[tgtTable].append(tgtColumn)
                    self.tablesSetSearch[tgtTable].add(tgtColumn)
                for srcPair in relations[relation]:
                    srcTable = srcPair[0]
                    srcColumn = srcPair[1]
                    self.currentSrcTables.add(srcTable)
                    if srcColumn not in self.tablesSetSearch[srcTable]:
                        self.tablesSetSearch[srcTable].add(srcColumn)
                        self.tablesSet[srcTable].append(srcColumn)

    def __getTablesColumnsRelations__(self, colLineages):
        tables = defaultdict(set)
        relations = defaultdict(set)

        for relation in colLineages:
            tgtTable = relation['TargetTable']
            tgtColumn = relation['TargetColumn']

            srcTable = relation['SourceTable']
            srcColumn = relation['SourceColumn']
            self.usedTables.add(srcTable)

            tables[tgtTable].add(tgtColumn)
            tables[srcTable].add(srcColumn)
            self.usedTables.add(tgtTable)

            relations[(tgtTable, tgtColumn)].add((srcTable, srcColumn))

        return {'tables': tables, 'relations': relations}

    def __getSQLLineage__(self,sqlState, ddlList):
        sqlObbj = parse_one(sqlState, "bigquery")
        self.__getPivotColumnsToRealColumn__(sqlObbj)
        (targTable,DBName) = self.__getTargetTable__(sqlObbj)
        if DBName:
            self.DBTableLookup[targTable] = DBName
        targCols = self.__getTargetTableColumns__(targTable, sqlObbj, ddlList)
        ls = []
        for ind in range(0, len(targCols)):
            srcColTable = self.__getSourceColumn__(ind, targCols[ind], sqlObbj, ddlList)
            if len(srcColTable) == 0 and isinstance(sqlObbj, exp.Insert):
                srcColTable = self.__getSourceColumn__(ind, targCols[ind], sqlObbj, ddlList,byName=False)

            for i in range(0, len(srcColTable)):
                ls.append(
                    {
                        'TargetTable': targTable,
                        'TargetColumn': targCols[ind],
                        'SourceTable': srcColTable[i][1],
                        'SourceColumn': srcColTable[i][0]
                    }
                )
            if len(srcColTable) == 0:
                ls.append(
                    {
                        'TargetTable': targTable,
                        'TargetColumn': targCols[ind],
                        'SourceTable': 'Hardcoded_Unknown',
                        'SourceColumn': 'Constant_Unknown'
                    }
                )
        return ls

    def __getPivotColumnsToRealColumn__(self,sqlObj):
        pv = list(sqlObj.find_all(exp.Pivot))
        if len(pv) == 0:
            return
        for ex in pv[0].expressions:
            for field in pv[0].args['field'].expressions:
                key = []
                if len(ex.alias_or_name) > 0:
                    key.append(exp.alias_or_name)
                key.append(field.alias_or_name)
                self.pivotColumn["_".join(key).upper()] = ex



    def __getSourceColumn__(self,ind, col, sqlObj, ddlList,byName=True):
        if col in self.varNames:
            return []
        ls = []
        sels = list(sqlObj.find_all(exp.Select))
        if len(sels) == 0:
            return None
        elif isinstance(sels[0].parent,exp.Union):
            unionParent = sels[0].parent
            colInd = -1
            for sel in sels:
                if sel.parent != unionParent:
                    continue
                if colInd > -1:
                    colObj = self.__getColumnByIndexFromSelect__(sel,colInd)
                else:
                    (colInd,colObj) = self.__getColumnByNameFromSelectInd__(sel, col)
                if not colObj:
                    continue
                colsls = self.__getColTableAliasList__(colObj)

                frJ = list(sel.find_all(exp.Subquery, exp.Table))
                for cc in colsls:
                    flagFound = False
                    tabl = cc[0].upper()
                    if len(tabl) > 0:
                        for fr in frJ:
                            if isinstance(fr, exp.Subquery) and tabl == fr.alias_or_name.upper():
                                temp = self.__getSourceColumn__(ind, cc[1], fr.this, ddlList,byName)
                                ls.extend(temp)
                                break
                            elif isinstance(fr, exp.Table) and \
                                    (tabl == fr.alias_or_name.upper() or (
                                            len(fr.alias_or_name) == 0 and tabl == fr.name.upper())):
                                ls.append((cc[1], fr.name.upper()))
                                if 'db' in fr.args and fr.args['db']:
                                    self.DBTableLookup[fr.name.upper()] = fr.args['db'].alias_or_name.upper()
                                break
                    else:
                        for fr in frJ:
                            if isinstance(fr, exp.Subquery):
                                temp = self.__getSourceColumn__(ind, cc[1], fr.this, ddlList,byName)
                                if len(temp) > 0:
                                    ls.extend(temp)
                                    break
                            elif isinstance(fr, exp.Table):
                                if cc[1].upper() in self.varNames:
                                    continue
                                if fr.name.upper() in ddlList and cc[1].upper() in ddlList[fr.name.upper()]:
                                    ls.append((cc[1], fr.name.upper()))
                                    flagFound = True
                                if 'db' in fr.args and fr.args['db']:
                                    self.DBTableLookup[fr.name.upper()] = fr.args['db'].alias_or_name.upper()

                            if len(frJ) == 1 and len(cc[0]) == 0:
                                ls.append((cc[1], fr.name.upper()))

                            if flagFound:
                                break
        else:
            if byName:
                colObj = self.__getColumnByNameFromSelect__(sels[0], col)
                if not colObj:
                    return []
            else:
                colObj = self.__getColumnByIndexFromSelect__(sels[0],ind)
                if not colObj:
                    return []
                else:
                    byName=True
            colsls = self.__getColTableAliasList__(colObj)

            frJ = list(sels[0].find_all(exp.Subquery, exp.Table))
            for cc in colsls:
                flagFound = False
                tabl = cc[0].upper()
                if len(tabl) > 0:
                    for fr in frJ:
                        if isinstance(fr, exp.Subquery) and tabl == fr.alias_or_name.upper():
                            temp = self.__getSourceColumn__(ind, cc[1], fr.this, ddlList,byName)
                            ls.extend(temp)
                            break
                        elif isinstance(fr, exp.Table) and \
                                (tabl == fr.alias_or_name.upper() or (
                                        len(fr.alias_or_name) == 0 and tabl == fr.name.upper())):
                            ls.append((cc[1], fr.name.upper()))
                            if 'db' in fr.args and fr.args['db']:
                                self.DBTableLookup[fr.name.upper()] = fr.args['db'].alias_or_name.upper()
                            break
                else:
                    for fr in frJ:
                        if isinstance(fr, exp.Subquery):
                            temp = self.__getSourceColumn__(ind, cc[1], fr.this, ddlList,byName)
                            if len(temp) > 0:
                                ls.extend(temp)
                                break
                        elif isinstance(fr, exp.Table):
                            if cc[1].upper() in self.varNames:
                                continue
                            if fr.name.upper() in ddlList and cc[1].upper() in ddlList[fr.name.upper()]:
                                ls.append((cc[1], fr.name.upper()))
                                flagFound = True

                            if 'db' in fr.args and fr.args['db']:
                                self.DBTableLookup[fr.name.upper()] = fr.args['db'].alias_or_name.upper()

                        if len(frJ) ==1 and len(cc[0]) ==0:
                            ls.append((cc[1], fr.name.upper()))

                        if flagFound:
                            break
        return list(set(ls))


    def __getColTableAliasList__(self,col):
        cols = list(col.find_all(exp.Column,exp.Var))
        def colToTuple(col):
            if isinstance(col,exp.Column):
                return (col.table.upper(), col.alias_or_name.upper())
            else:
                return ("", col.alias_or_name.upper())
        return list(map(colToTuple, cols))

    def __getColumnByNameFromSelect__(self,sel, colName):
        for col in sel.selects:
            aliass = list(col.find_all(exp.Alias))
            if len(aliass) > 0:
                for a in aliass:
                    if a.alias_or_name.upper() == colName:
                        return a
            else:
                cols = list(col.find_all(exp.Column,exp.Var))
                for c in cols:
                    if c.alias_or_name.upper() == colName:
                        return col
        if self.pivotColumn:
            if colName in self.pivotColumn:
                return self.pivotColumn[colName]
        return None

    def __getColumnByNameFromSelectInd__(self,sel, colName):
        ind = 0
        for col in sel.selects:
            if col.alias_or_name.upper() == colName:
                return (ind,col)
            ind = ind+1
        return (-1,None)

    def __getColumnByIndexFromSelect__(self,sel, colInd):
        if colInd >= len(sel.selects):
            return None
        return sel.selects[colInd]



    def __getTargetTable__(self,sqllotObj):
        tableName = None
        DBName = self.defaultDB
        if isinstance(sqllotObj, exp.Create) or isinstance(sqllotObj, exp.Insert):
            current = sqllotObj.this
            while current and not isinstance(current, exp.Table):
                current = current.this
            if current:
                table: exp.Table = current
                tableName = table.alias_or_name
                if 'db' in table.args and table.args['db']:
                    DBName = table.args['db'].alias_or_name
        return (tableName.upper(),DBName.upper())

    def __getTargetTableColumns__(self,targetTableName, sqlObj, ddllist):
        cols = []
        if isinstance(sqlObj, exp.Insert) and isinstance(sqlObj.this, exp.Schema):
            obj: exp.Schema = sqlObj.this
            for col in obj.expressions:
                cols.append(col.alias_or_name.upper())
        elif isinstance(sqlObj, exp.Insert) and isinstance(sqlObj.this, exp.Table):
            ddl = ddllist[targetTableName.upper()]
            cols.extend(ddl)
        elif isinstance(sqlObj, exp.Create):
            sels = list(sqlObj.find_all(exp.Subquery))
            if len(sels) > 0:
                selTarget: exp.Subquery = None
                for sel in sels:
                    selTarget = sel
                    if selTarget.parent == sqlObj:
                        break
                selQuery: exp.Select = selTarget.this
                for col in selQuery.selects:
                    cols.append(col.alias_or_name.upper())
            else:
                sels = list(sqlObj.find_all(exp.Select))
                if len(sels) > 0 :
                    selTarget: exp.Subquery = None
                    for sel in sels:
                        selTarget = sel
                        if selTarget.parent == sqlObj:
                            break
                    selQuery: exp.Select = selTarget
                    for col in selQuery.selects:
                        cols.append(col.alias_or_name.upper())
        return cols

    def __replaceStarInQuery__(self,col: exp.Column, ddlList):
        parent_sel = col.parent_select
        fromJoin = list(parent_sel.find_all(exp.From, exp.Join))
        ls = []
        for f in fromJoin:
            if f.parent_select == parent_sel:
                if isinstance(f, exp.From) and len(f.expressions) > 0 and isinstance(f.expressions[0], exp.Table):
                    ls.extend(ddlList[f.expressions[0].this.alias_or_name.upper()])
                elif isinstance(f, exp.From) and isinstance(f.this, exp.Table):
                    ls.extend(ddlList[f.this.name.upper()])
                elif isinstance(f, exp.Join) and isinstance(f.this, exp.Join):
                    ls.extend(ddlList[f.this.name.upper()])
                else:
                    fromObj = None
                    if isinstance(f, exp.From) and len(f.expressions) > 0:
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
                            if isinstance(coli.this, exp.Star) or isinstance(coli, exp.Star):
                                cols = self.__replaceStarInQuery__(coli, ddlList)
                                ls.extend(cols)
                            else:
                                ls.append(coli.alias_or_name)

        return ls

    def __replaceStarInScript__(self,sqlState, ddlList):
        sqlObj = parse_one(sqlState, "bigquery")
        #print(sqlObj)
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

        return sqlObj.sql(dialect="bigquery")

    def __convertCreateSelectToSubuery__(self,stmt):
        if "INSERT " in "".strip():
            return stmt
        res = []
        state = 0
        tokens = re.split(" |\n",stmt)
        for token in tokens:
            tok = token.strip().upper()
            if tok == "CREATE":
                state = 1
            elif tok == "AS" and state == 1:
                state = 2
            elif (tok == "SELECT" or tok == "WITH") and state == 2:
                res.append(" ( ")
                state = 3
            elif tok == "(" and state == 2:
                return stmt

            res.append(token)
        if state == 3:
            res.append(" )")
        return " ".join(res)


    def __removePartitionBy2__(self, sql):
        stmt = sqlparse.parse(sql)
        tokens: TokenList = stmt[0].tokens
        flags=0
        ls = []
        for tok in tokens:
            temp = self.__removeRecPrtition__(tok,flags)
            ls.extend(temp[0])
            flags = temp[1]

        return "".join(ls)

    def __removeRecPrtition__(self,tok:Token,flags):
        ls = []
        if hasattr(tok,"tokens"):
            for tt in tok.tokens:
                temp = self.__removeRecPrtition__(tt,flags)
                ls.extend(temp[0])
                flags = temp[1]
        else:
            if tok.is_keyword and "CREATE" in tok.value.upper():
                flags = 1
                ls.append(str(tok))
            elif tok.is_keyword and tok.value.upper() == "PARTITION" and flags==1:
                flags=2
            elif tok.is_keyword and tok.value.upper() == "BY" and flags==2:
                flags=3
            elif tok.is_keyword and tok.value.upper() == "AS":
                flags=4
                ls.append(str(tok))
            elif flags != 3:
                ls.append(str(tok))

        return (ls,flags)


    def __removePartitionBy__(self,sql):
        # Review and fix
        ################
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
            elif (isinstance(c, Identifier) or isinstance(c,Function)) and isBy:
                c3 = c
                isPartition = False
                isBy = False
                ls.extend(map(str, c.tokens[1:]))
                continue
            ls.append(str(c))
        return "".join(ls)

    def __getPartial__(self,tokens):
        res = []
        for curr in tokens:
            if hasattr(curr, 'tokens'):
                res.extend(self.__getPartial__(curr.tokens))
            elif curr.ttype != T.Comment.Multiline and curr.ttype != T.Comment.Single:
                res.append(str(curr))
        return res

    def __removeComments__(self,sql):
        sp = sqlparse.parse(sql)
        res = self.__getPartial__(sp[0].tokens)
        return "".join(res)

    def __convertCTEtoSubqueries__(self,sqlStmt):
        def transformer(node):
            if isinstance(node, exp.Table) and node.name.upper() in dictCTE:
                return parse_one("({}) AS {}".format(dictCTE[node.name.upper()].sql(dialect="bigquery"), node.alias_or_name), "bigquery")
            return node

        def transformerNoWith(node):
            if isinstance(node, exp.Select):
                node.args['with'] = None
            elif isinstance(node,exp.With):
                return None
            if hasattr(node,"ctes"):
                node.ctes.clear()
            return node

        sqlObj = parse_one(sqlStmt, "bigquery")
        dictCTE = {}
        ctes = list(sqlObj.find_all(exp.CTE))
        if len(ctes) == 0:
            return sqlStmt
        for cte in ctes:
            dictCTE[cte.alias_or_name.upper()] = cte.this.transform(transformer)

        sels = list(sqlObj.find_all(exp.Select))
        mainSelect = sels[0]

        transformed_tree = sqlObj.transform(transformer)
        transformed_tree = transformed_tree.transform(transformerNoWith)
        # transformed_tree.args['with'] = None
        return transformed_tree.sql(dialect="bigquery")

    def __readSql__(self, entrytableName):
        fullPath = "{}/{}.sql".format(self.sqlPath, entrytableName)
        if not (os.path.exists(fullPath)):
            return None
        with open(fullPath, 'r') as file:
            lines = file.readlines()

        gg =  "".join(lines)
        dd = sqlparse.parse(gg)
        ll = []
        for stmt in dd:
            stmt1 = self.__removeComments__(str(stmt))
            if len(stmt1.strip()) > 0:
                ll.append(stmt1)
        return "".join(ll)

    def getLineageDeep(self,targetTable,verbose=False):
        from copy import deepcopy
        ls = [targetTable]
        linListIntial = []
        linListFiltered = []
        visited = set()
        while len(ls) > 0:
            current = ls.pop()
            if current in visited:
                continue
            if verbose:
                print("*** Getting Lineage for {} ***".format(current))
            linFlag = self.getLineage(current,verbose=verbose)
            if linFlag:
                self.createfilteredRelations(current, ["VFPT_DH_LAKE_EDW_STAGING_S"])
                self.linTablesList.append(current)
                linListIntial.append(deepcopy(self.relationsSet))
                linListFiltered.append(deepcopy(self.relationsSetNew))
                ls.extend(list(self.currentSrcTables))
                self.relationsSetNew.clear()
                self.relationsSet.clear()

            visited.add(current)
        #if len(self.linTablesList) > 0:
        #    for table in self.linTablesList:
        #        self.createfilteredRelations(table,["VFPT_DH_LAKE_EDW_STAGING_S"])
        #if len(self.linTablesList) > 0:
        #    return True
        """
        pos = self.createGraphvizDeep(targetTable,self.linTablesList,linListFiltered,"./",None,rankSep=3,nodeSep=1.5)
        self.writeGraphvizToPNG("Tab4.png")
        self.generateDrawIOXMLLayoutDeepNetworkX(self.linTablesList,linListFiltered,pos,
                                                 "shape=swimlane;fontStyle=0;childLayout=stackLayout;horizontal=1;startSize=26;horizontalStack=0;resizeParent=1;resizeParentMax=0;resizeLast=0;collapsible=1;marginBottom=0;align=center;fontSize=14;fillColor=#60a917;strokeColor=#2D7600;fontColor=#ffffff;",
                                                 "text;spacingLeft=4;spacingRight=4;overflow=hidden;rotatable=0;points=[[0,0.5],[1,0.5]];portConstraint=eastwest;fontSize=12;whiteSpace=wrap;html=1;fillColor=#f5f5f5;fontColor=#333333;strokeColor=#666666;gradientColor=#b3b3b3;",
                                                 "rounded=0;orthogonalLoop=1;jettySize=auto;html=1;orthogonal=1;edgeStyle=orthogonalEdgeStyle;curved=1;",
                                                 "./", "F_SUBSCRIBER_BASE_SEMANTIC_M.drawio",collapsed=True
                                                 )
        """
        return (self.linTablesList,linListFiltered)

    def assignXY(self,tempRelationsList):
        dictColumns = defaultdict(set)
        rela = []
        for tempRelations in tempRelationsList:
            for relation in tempRelations.keys():
                tgtTable = relation[0]
                tgtColumn = relation[1]
                dictColumns[tgtTable].add(tgtColumn)
                for src in tempRelations[relation]:
                    srcTable = src[0]
                    dictColumns[tgtTable].add(srcTable)
                    rela.append((tgtTable,srcTable))
                    dictColumns[srcTable].add('')


        import networkx as nx
        #import matplotlib as mpl
        #import matplotlib.pyplot as plt
        G = nx.Graph()
        G.add_nodes_from(dictColumns.keys())
        G.add_edges_from(rela)
        pos = nx.nx_pydot.graphviz_layout(G,prog="dot",root = G.nodes["F_SUBSCRIBER_BASE_SEMANTIC_M"])
        #nx.draw_networkx(G, pos, node_size=10000)
        #ax = plt.gca()
        #ax.set_axis_off()
        #plt.show()
        return pos

    def createGraphvizDeep(self,entryTableName,linTablesList,linRelations,templateFullPath,templateFileName,
                           bgColor="#FFFFFF",fontName="Arial",nodeSep=0.5,rankSep=5,headerColor="#96be5c",
                           edgStrokColor="#aeaeae",useFiltered=False):

        if templateFullPath and len(templateFullPath) > 0 and templateFileName and len(templateFileName) > 0:
            self.diagram = LineageDiagram(entryTableName, "{}/{}".format(templateFullPath, templateFileName))
        else:
            self.diagram = LineageDiagram(entryTableName, None)
        self.diagram.createGraph(bgColor,fontName,nodeSep,rankSep)
        usedT = self.usedTables

        dictColumns = defaultdict(set)
        for ind in range(0, len(linTablesList)):
            tempRelations = linRelations[ind]
            for relation in tempRelations.keys():
                tgtTable = relation[0]
                tgtColumn = relation[1]
                dictColumns[tgtTable].add(tgtColumn)
                for src in tempRelations[relation]:
                    srcTable = src[0]
                    srcColumn = src[1]
                    dictColumns[srcTable].add(srcColumn)

        for tableName in dictColumns.keys():
            db = self.DBTableLookup[tableName]
            tableHeaderColor = headerColor
            if not tableHeaderColor:
                tableHeaderColor = self.DEFAULT_TABLE_HEADER
            self.diagram.createNode(tableHeaderColor, tableName, sorted(list(dictColumns[tableName])))

        for ind in range(0, len(linTablesList)):
            tempRelations = linRelations[ind]
            for relation in tempRelations.keys():
                tgtTable = relation[0]
                tgtColumn = relation[1]
                for src in tempRelations[relation]:
                    srcTable = src[0]
                    srcColumn = src[1]
                    if edgStrokColor and len(edgStrokColor) > 0:
                        edgColor = edgStrokColor
                    else:
                        edgColor = self.DEFAULT_EDGE
                    self.diagram.createEdge(edgColor,srcTable,srcColumn,tgtTable,tgtColumn)
        return self.diagram.getNodesPos()

    def generateDrawIOXMLLayoutDeepNetworkX(self, linTablesList,
                                            linRelations,
                                            nodesPos,
                                            tableStyle,
                                            columnStyle,
                                            edgeStyle,
                                            outputPath,
                                            outputFileName,
                                            srcfactorSpace=2,
                                            srcTgtSpaceFactor=4,
                                            distinceSrcTgt=200,
                                            itemHieght=25,
                                            collapsed=False,
                                            isInteractive=True,
                                            strokeColor="#f51919",
                                            useFiltered=False):

        lin = LineageToDrawIO(tableStyle, columnStyle, edgeStyle)


        for ind in range(0, len(linTablesList)):
            tempRelations = linRelations[ind]
            targetTableName = linTablesList[ind]
            totalColumns = 0
            dictColumns = defaultdict(set)
            for relation in tempRelations.keys():
                tgtTable = relation[0]
                tgtColumn = relation[1]
                dictColumns[tgtTable].add(tgtColumn)
                totalColumns += 1
                for src in tempRelations[relation]:
                    srcTable = src[0]
                    srcColumn = src[1]
                    dictColumns[srcTable].add(srcColumn)
                    totalColumns += 1
            totalColumns += len(dictColumns.keys())  ## number of tables as headers
            for tableName in dictColumns.keys():
                X = nodesPos[tableName][0]
                Y = nodesPos[tableName][1]
                lin.addTable(tableName, list(sorted(dictColumns[tableName])),
                             X, Y, collapsed=collapsed)

            for relation in tempRelations.keys():
                tgtTable = relation[0]
                tgtColumn = relation[1]
                for src in tempRelations[relation]:
                    srcTable = src[0]
                    srcColumn = src[1]
                    lin.addEdge(srcTable, srcColumn, tgtTable, tgtColumn)

        if isInteractive:
            lin.addInteractionToDiagram(targetTableName, strokeColor)

        lin.saveToFile(outputPath, outputFileName)

    def generateDrawIOXMLLayoutDeep(self,linTablesList,
                                    linRelations,
                                    tableStyle,
                                    columnStyle,
                                    edgeStyle,
                                    outputPath,
                                    outputFileName,
                                    srcfactorSpace=2,
                                    srcTgtSpaceFactor=4,
                                    distinceSrcTgt=200,
                                    itemHieght=25,
                                    collapsed=False,
                                    isInteractive = True,
                                    strokeColor = "#f51919",
                                    useFiltered=False):
        lin = LineageToDrawIO(tableStyle, columnStyle, edgeStyle)
        """
        Create dot using draing Graphviz and get position and use it to draw drawio
        """
        colsList = defaultdict(list)
        previousSrcTableEndY = 0
        previousSrcTableEndX = 0
        reversedLinTablesList = linTablesList[::-1]
        reversedLinRelations = linRelations[::-1]
        dictColumns = defaultdict(set)
        for ind in range(0,len(reversedLinTablesList)):
            tempRelations = reversedLinRelations[ind]
            targetTableName = reversedLinTablesList[ind]
            totalColumns = 0
            #dictColumns = defaultdict(set)
            for relation in tempRelations.keys():
                tgtTable = relation[0]
                tgtColumn = relation[1]
                dictColumns[tgtTable].add(tgtColumn)
                totalColumns+=1
                for src in tempRelations[relation]:
                    srcTable = src[0]
                    srcColumn = src[1]
                    dictColumns[srcTable].add(srcColumn)
                    totalColumns += 1
            totalColumns+=len(dictColumns.keys())  ## number of tables as headers

            for tableName in dictColumns.keys():
                if tableName == targetTableName:
                    srcColumns = totalColumns - len(dictColumns[tableName])-1
                    intial_y = int(srcColumns/4) *itemHieght
                    y = intial_y - int(((len(dictColumns[tableName])+1)*itemHieght) /4)
                    lin.addTable(tableName, list(sorted(dictColumns[tableName])),
                                 previousSrcTableEndX + distinceSrcTgt*srcTgtSpaceFactor, y,collapsed=collapsed)


                else:
                    lin.addTable(tableName, list(sorted(dictColumns[tableName])),
                                 previousSrcTableEndX,previousSrcTableEndY,collapsed=collapsed)
                    previousSrcTableEndY+= ((srcfactorSpace+len(dictColumns[tableName])) * itemHieght)


            previousSrcTableEndX += distinceSrcTgt*srcTgtSpaceFactor
            for relation in tempRelations.keys():
                tgtTable = relation[0]
                tgtColumn = relation[1]
                for src in tempRelations[relation]:
                    srcTable = src[0]
                    srcColumn = src[1]
                    lin.addEdge(srcTable, srcColumn, tgtTable, tgtColumn)


            if isInteractive:
                lin.addInteractionToDiagram(str(ind),targetTableName,strokeColor)

        for tableName in dictColumns.keys():
            table = lin.__checkTableExist__(tableName)
            currentX = table.mxCell.mxGeometry.attrib["x"]
            colsList[currentX].append(table)


        print(colsList.keys())
        for xInd in colsList.keys():
            prevEndY = 0
            lsCols = colsList[xInd]
            lsCols.sort(key = lambda x: int(table.mxCell.mxGeometry.attrib["y"]))
            for table in lsCols:
                tableName = table.attrib["name"]
                currentY = table.mxCell.mxGeometry.attrib["y"]
                newY  = prevEndY + (srcfactorSpace * itemHieght)
                if int(currentY) < prevEndY:
                    table.mxCell.mxGeometry.attrib["y"] = str(newY)
                prevEndY = prevEndY + ((srcfactorSpace + len(dictColumns[tableName])) * itemHieght)

        lin.saveToFile(outputPath, outputFileName)



if __name__ == "__main__":
    ln = QueryLineageAnalysis("./", "./DDL",defaultDB = "VFPT_DH_LAKE_EDW_STAGING_S",isDebug=True)
    (linTables,linRelations)=ln.getLineageDeep("F_SUBSCRIBER_BASE_EVENT_D",True)
    ln.generateDrawIOXMLLayoutDeep(linTables,linRelations,
                                   "shape=swimlane;fontStyle=0;childLayout=stackLayout;horizontal=1;startSize=26;horizontalStack=0;resizeParent=1;resizeParentMax=0;resizeLast=0;collapsible=1;marginBottom=0;align=center;fontSize=14;fillColor=#60a917;strokeColor=#2D7600;fontColor=#ffffff;",
                                   "text;spacingLeft=4;spacingRight=4;overflow=hidden;rotatable=0;points=[[0,0.5],[1,0.5]];portConstraint=eastwest;fontSize=12;whiteSpace=wrap;html=1;fillColor=#f5f5f5;fontColor=#333333;strokeColor=#666666;gradientColor=#b3b3b3;",
                                   "rounded=0;orthogonalLoop=1;jettySize=auto;html=1;orthogonal=1;edgeStyle=orthogonalEdgeStyle;curved=1;",
                                   "./","F_SUBSCRIBER_BASE_EVENT_D.drawio",collapsed=True
                                   )
    #ln.createGraphviz("F_SUBSCRIBER_BASE_SEMANTIC_M","./",None,rankSep=30,useFiltered=True)
    #ln.writeGraphvizToPNG("Tab4.png")

    #ln.createfilteredRelations("F_SUBSCRIBER_BASE_SEMANTIC_D",["VFPT_DH_LAKE_EDW_STAGING_S"])
    #ln.createGraphviz("Test","./",None,True)
    #ln.writeGraphvizToPNG("Tab4.png")
    #ln.generateDrawIOCSV("./","Tab4.txt","tableBox","tableColumn","./","tab4_drawio.txt",True)
    #ln.generateDrawIOXMLLayout("F_SUBSCRIBER_BASE_SEMANTIC_D",
    #                           "shape=swimlane;fontStyle=0;childLayout=stackLayout;horizontal=1;startSize=26;horizontalStack=0;resizeParent=1;resizeParentMax=0;resizeLast=0;collapsible=1;marginBottom=0;align=center;fontSize=14;fillColor=#60a917;strokeColor=#2D7600;fontColor=#ffffff;",
    #                           "text;spacingLeft=4;spacingRight=4;overflow=hidden;rotatable=0;points=[[0,0.5],[1,0.5]];portConstraint=eastwest;fontSize=12;whiteSpace=wrap;html=1;fillColor=#f5f5f5;fontColor=#333333;strokeColor=#666666;gradientColor=#b3b3b3;",
    #                           "rounded=0;orthogonalLoop=1;jettySize=auto;html=1;exitX=1;exitY=0.5;exitDx=0;exitDy=0;entryX=0;entryY=0.5;entryDx=0;entryDy=0;orthogonal=1;edgeStyle=orthogonalEdgeStyle;curved=1;",
    #                           "./","F_SUBSCRIBER_BASE_SEMANTIC_D.drawio",useFiltered=True)
