import os
from collections import defaultdict
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
from lineage_diagram import lineageDiagram
from drawio_gen import DrawIOLineageGenerator
""" 
Queries in the path with name as target table + .sql
"""


class QueryLineageAnalysis:
    CONFIG_FILE_NAME = "lineage.config"
    DEFAULT_TABLE_HEADER = "#96be5c"

    def __init__(self, sqlPath, DDLPath, configPath):
        self.sqlPath = sqlPath
        self.DDLPath = DDLPath
        self.configPath = configPath
        self.tablesSet = defaultdict(set)
        self.relationsSet = defaultdict(set)
        self.usedTables=set()
        self.DBTableLookup = defaultdict(lambda: 'DEFAULT')
        self.keywordsList=set(["CREATE","INSERT"])
        self.config = configparser.SafeConfigParser()
        self.__readConfigFile__()
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
                        self.tablesSet[table['table_name']].add(column['name'])


    def createGraphviz(self,entryTableName,templateFullPath,templateFileName):
        self.diagram = lineageDiagram(entryTableName,"{}/{}".format(templateFullPath,templateFileName))
        self.diagram.createGraph()
        for table in self.tablesSet.keys():
            if table not in self.usedTables:
                continue
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


    def writeGraphvizToPNG(self,fullPath):
        self.diagram.saveGraphAsPNG(fullPath)

    def generateDrawIOCSV(self,templatePath,templateFileName,
                          tableStyleName,columnStyleName,
                          outputPath,outputFileName):
        drawgen = DrawIOLineageGenerator(templatePath,templateFileName,tableStyleName,columnStyleName)
        for relation in self.relationsSet.keys():
            tgtTable = relation[0]
            tgtColumn = relation[1]
            drawgen.addTable(tgtTable)
            for src in self.relationsSet[relation]:
                srcTable = src[0]
                srcColumn = src[1]
                drawgen.addTable(srcTable)
                drawgen.addColumn(srcTable,srcColumn,tgtTable,tgtColumn)
                drawgen.addColumn(tgtTable,tgtColumn,None,None)
                #drawgen.addColumn(srcTable,srcColumn,None,None)
                #drawgen.addColumn(tgtTable,tgtColumn,srcTable,srcColumn)
        drawgen.generateCSVDrawIO(outputPath,outputFileName)







    def __readConfigFile__(self):
        self.config.read("{}/{}".format(self.configPath, self.CONFIG_FILE_NAME))

    def __getConfigItem__(self, section, item):
        if section in self.config and item in self.config[section]:
            return self.config[section][item]
        else:
            return None

    def filterRelations(self,targetTable,dbListExclude):
        relationsSetNew = defaultdict(list)
        for relation in self.relationsSet.keys():
            tgtTable = relation[0]
            if tgtTable == targetTable:
                relationsSetNew[relation].extend(self.__getSourcePair__(relation,dbListExclude))
        return relationsSetNew
    def __getSourcePair__(self,srcPairInput,dbListExclude):
        dbListExcludeSet = set(dbListExclude)
        ls = set()
        for srcPair in self.relationsSet[srcPairInput]:
            if self.DBTableLookup[srcPair[0]] in dbListExcludeSet:
                pairs = self.__getSourcePair__(srcPair,dbListExcludeSet)
                ls.update(pairs)
            else:
                ls.add(srcPair)

        return ls




    def isStmtOK(self,stmt):
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

    def getLineage(self, entrytableName):
        raw_sql = self.__readSql__(entrytableName)
        if not (raw_sql):
            return None
        statements = raw_sql.split(";")
        for stmt in statements:
            stmt = stmt.strip()
            if len(stmt) == 0 or not self.isStmtOK(stmt):
                continue
            sql0 = self.__removeComments__(stmt)
            #sql0 = self.__removePartitionBy__(sql0)
            sql0 = self.__removePartitionBy2__(sql0)
            sql0 = sql0.replace("`", "")
            sql0 = sql0.replace("-", "_")
            sql0 = self.__convertCreateSelectToSubuery__(sql0)
            sfg = self.__convertCTEtoSubqueries__(sql0)
            lsff = self.__replaceStarInScript__(sfg, self.tablesSet)
            lin = self.__getSQLLineage__(str(lsff), self.tablesSet)
            tablesRelations = self.__getTablesColumnsRelations__(lin)
            tables = tablesRelations["tables"]
            relations = tablesRelations["relations"]
            self.__updateTablesRelations__(tables, relations)

    def __updateTablesRelations__(self,tables,relations):
        for table in tables.keys():
            if table not in self.tablesSet:
                self.tablesSet[table].update(tables[table])

        for relation in relations.keys():
            if relation not in self.relationsSet:
                self.relationsSet[relation] = relations[relation]

    def __getTablesColumnsRelations__(self, colLineages):
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
        (targTable,DBName) = self.__getTargetTable__(sqlObbj)
        if DBName:
            self.DBTableLookup[targTable] = DBName
        targCols = self.__getTargetTableColumns__(targTable, sqlObbj, ddlList)
        ls = []
        for ind in range(0, len(targCols)):
            if isinstance(sqlObbj, exp.Insert):
                sels = list(sqlObbj.find_all(exp.Select))
                col = sels[0].selects[ind].alias_or_name.upper()
                srcColTable = self.__getSourceColumn__(ind, col, sqlObbj, ddlList)
            else:
                srcColTable = self.__getSourceColumn__(ind, targCols[ind], sqlObbj, ddlList)
            # srcTable = getSourceTable(ind,sqlObbj,ddlList)
            # if len(srcTable) == 0:
            #    srcTable=["CONSTANT"]
            for i in range(0, len(srcColTable)):
                ls.append(
                    {
                        'TargetTable': targTable,
                        'TargetColumn': targCols[ind],
                        'SourceTable': srcColTable[i][1],
                        'SourceColumn': srcColTable[i][0]
                    }
                )
        return ls

    def __getSourceColumn__(self,ind, col, sqlObj, ddlList):
        """
        if column has prefix so check if from with alias
        else loop on all from recorive to get columns
        """
        ls = []
        sels = list(sqlObj.find_all(exp.Select))
        if len(sels) == 0:
            return None
        elif isinstance(sels[0].parent,exp.Union):
            colInd = -1
            for sel in sels:
                if colInd > -1:
                    colObj = self.__getColumnByIndexFromSelect__(sel,colInd)
                else:
                    (colInd,colObj) = self.__getColumnByNameFromSelectInd__(sel, col)
                if not colObj:
                    return []
                colsls = self.__getColTableAliasList__(colObj)

                frJ = list(sel.find_all(exp.Subquery, exp.Table))
                for cc in colsls:
                    tabl = cc[0].upper()
                    if len(tabl) > 0:
                        for fr in frJ:
                            if isinstance(fr, exp.Subquery) and tabl == fr.alias_or_name.upper():
                                temp = self.__getSourceColumn__(ind, cc[1], fr.this, ddlList)
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
                                temp = self.__getSourceColumn__(ind, cc[1], fr.this, ddlList)
                                if len(temp) > 0:
                                    ls.extend(temp)
                                    break
                            elif isinstance(fr, exp.Table):
                                if fr.name.upper() in ddlList and cc[1].upper() in ddlList[fr.name.upper()]:
                                    ls.append((cc[1], fr.name.upper()))
                                if 'db' in fr.args and fr.args['db']:
                                    self.DBTableLookup[fr.name.upper()] = fr.args['db'].alias_or_name.upper()
                            if len(frJ) == 1 and len(cc[0]) == 0:
                                ls.append((cc[1], fr.name.upper()))
        else:
            colObj = self.__getColumnByNameFromSelect__(sels[0], col)
            if not colObj:
                return []
            colsls = self.__getColTableAliasList__(colObj)

            frJ = list(sels[0].find_all(exp.Subquery, exp.Table))
            for cc in colsls:
                tabl = cc[0].upper()
                if len(tabl) > 0:
                    for fr in frJ:
                        if isinstance(fr, exp.Subquery) and tabl == fr.alias_or_name.upper():
                            temp = self.__getSourceColumn__(ind, cc[1], fr.this, ddlList)
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
                            temp = self.__getSourceColumn__(ind, cc[1], fr.this, ddlList)
                            if len(temp) > 0:
                                ls.extend(temp)
                                break
                        elif isinstance(fr, exp.Table):
                            if fr.name.upper() in ddlList and cc[1].upper() in ddlList[fr.name.upper()]:
                                ls.append((cc[1], fr.name.upper()))
                            if 'db' in fr.args and fr.args['db']:
                                self.DBTableLookup[fr.name.upper()] = fr.args['db'].alias_or_name.upper()
                        if len(frJ) ==1 and len(cc[0]) ==0:
                            ls.append((cc[1], fr.name.upper()))

        return list(set(ls))

    def __getColList__(self,col):
        cols = list(col.find_all(exp.Column))
        return list(map(lambda X: X.alias_or_name, cols))

    def __getColTableAliasList__(self,col):
        cols = list(col.find_all(exp.Column))
        return list(map(lambda X: (X.table.upper(), X.alias_or_name.upper()), cols))

    def __getColumnByNameFromSelect__(self,sel, colName):
        for col in sel.selects:
            if col.alias_or_name.upper() == colName:
                return col
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


    # first version explicit column mention in the select
    def __getSourceTable__(self,ind, sqlObj, ddlList):
        sels = list(sqlObj.find_all(exp.Select))
        col = sels[0].selects[ind]
        if isinstance(col, exp.Alias):
            return self.__getSrouceTableSelects__('', col.this.alias_or_name.upper(), sels[0], ddlList)
        else:
            return self.__getSrouceTableSelects__('', col.alias_or_name.upper(), sels[0], ddlList)

    # fix for TAB1
    def __getSrouceTableSelects__(self,tableAlias, col, sqlObj, ddlList):
        tableNames = []
        sels = list(sqlObj.find_all(exp.Select))
        selTgt = sels[0]
        fromJoin = list(selTgt.find_all(exp.From, exp.Join))
        for f in fromJoin:
            if f.parent_select == selTgt:
                if isinstance(f, exp.From) and isinstance(f.expressions[0], exp.Table):
                    if tableAlias and len(tableAlias) > 0 and len(f.expressions[0].alias_or_name) > 0:
                        if tableAlias.upper() == f.expressions[0].alias_or_name.upper():
                            tableNames.append(f.expressions[0].this.alias_or_name.upper())
                    else:
                        if col.upper() in ddlList[f.expressions[0].this.alias_or_name.upper()]:
                            tableNames.append(f.expressions[0].this.alias_or_name.upper())
                elif isinstance(f, exp.Join) and f.this and isinstance(f.this, exp.Table):
                    if tableAlias and len(tableAlias) > 0 and len(f.this.alias_or_name) > 0:
                        if tableAlias.upper() == f.this.alias_or_name.upper():
                            tableNames.append(f.this.this.alias_or_name.upper())
                    else:
                        if col.upper() in ddlList[f.this.this.alias_or_name.upper()]:
                            tableNames.append(f.this.this.alias_or_name.upper())
                else:
                    sss = list(f.find_all(exp.Select))
                    cc = None
                    for column in sss[0].selects:
                        if column.alias_or_name.upper() == col:
                            cc = column
                            break
                    if cc:
                        newCols = self.__getColTableAliasList__(cc)
                        for newCol in newCols:
                            tableName = self.__getSrouceTableSelects__(newCol[0], newCol[1], sss[0], ddlList)
                            tableNames.extend(tableName)

        return tableNames

    def __getSourceTableColmnList__(self,trgtCols, sqlObj, ddlList):
        dict = {}
        for i in range(0, len(trgtCols)):
            srcTable = self.__getSourceTable__(i, sqlObj, ddlList)
            srcCol = self.__getSourceColumn__(i, trgtCols[i], sqlObj)
            dict[trgtCols[i]] = (srcTable, srcCol)
        return dict

    def __getTargetTable__(self,sqllotObj):
        tableName = None
        DBName = None
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
                if isinstance(f, exp.From) and isinstance(f.expressions[0], exp.Table):
                    ls.extend(ddlList[f.expressions[0].this.alias_or_name.upper()])
                elif isinstance(f, exp.Join) and isinstance(f.this, exp.Join):
                    ls.extend(ddlList[f.this.alias_or_name.upper()])
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
                            if isinstance(coli.this, exp.Star) or isinstance(coli, exp.Star):
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

    def __convertCreateSelectToSubuery__(self,stmt):
        """
        Convert Create without brackets to have brackets
        :param Stmt:
        :return:
        """
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
            elif tok == "SELECT" and state == 2:
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
            if isinstance(node, exp.Table) and node.name in dictCTE:
                return parse_one("({}) AS {}".format(str(dictCTE[node.name]), node.alias_or_name), "bigquery")
            return node

        def transformerNoWith(node):
            if isinstance(node, exp.Select):
                node.args['with'] = None
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
        return transformed_tree.sql()

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


if __name__ == "__main__":
    frf="\nsfsfd\nsdfsd\n".strip()
    #sqlPath, DDLPath, templateFullPath, templateFileName, configPath
    ln = QueryLineageAnalysis("./", None, "./")
    ln.getLineage("Test")
    dfdfd = ln.filterRelations("F_SUBSCRIBER_BASE_SEMANTIC_D",["VFPT_DH_LAKE_EDW_STAGING_S"])
    ln.createGraphviz("Test","./",None)
    ln.writeGraphvizToPNG("Tab4.png")
    ln.generateDrawIOCSV("./","Tab4.txt","tableBox","tableColumn","./","tab4_drawio.txt")
    """
    We need to find solution for select with union 
    """


    """
    Queries in the insert must have aliases same as column names of the target table

    ##### REsolve if more than one subquery with same name should each one to be unique
    """

