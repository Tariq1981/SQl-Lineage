import argparse
from configparser import ConfigParser
from querylineage2 import QueryLineageAnalysis

if __name__ == "__main__":
    """
    Do all generation in one go and process
    and call the drawio to export to html
    """
    argParser = argparse.ArgumentParser()
    argParser.add_argument("-p","--path",help="full path for lineage_config.ini including filename")
    args = argParser.parse_args()
    parser = ConfigParser()
    parser.read(args.path)
    isDebug = False
    deb = parser.get("lineage","debug")
    deb_path = parser.get("lineage","debug_log")
    verboseStr = parser.get("lineage","verbose")
    verbose = False
    if verboseStr and verboseStr.lower() == "true":
        verbose = True
    if deb and deb.lower() == "true":
        isDebug = True
        if not deb_path:
            deb_path = "./"
    sqlPath = parser.get("lineage","sql_path")
    ddlPath = parser.get("lineage","ddl_path")
    targetTableName = parser.get("lineage","target_table_name")
    filterTables = parser.get("lineage","is_filter_intermediate").lower()
    dbList = parser.get("lineage", "intermediate_databases")
    graphType = parser.get("graph", "type")
    outPath = parser.get("graph","output_path")
    defaultDB = parser.get("lineage","defaultDB")
    ln = QueryLineageAnalysis(sqlPath, ddlPath,defaultDB,isDebug,deb_path)
    ln.getLineage(targetTableName.upper(),verbose)
    filter = False
    if filterTables == "true":
        filter = True
        #print("Filtered !!!!!!")
        filtedDBs = dbList.split(",")
        filtDBs = list(map(lambda x:x.upper(),filtedDBs))
        ln.createfilteredRelations(targetTableName.upper(), filtDBs)

    if graphType.lower() == "graphviz":
        templatePath = parser.get("graphviz", "templatepath")
        templateFileName = parser.get("graphviz", "templateFileName")
        bgColor = parser.get("graphviz","background_color")
        fontName = parser.get("graphviz","font_name")
        srcSep = float(parser.get("graphviz","vertical_sep"))
        tgtSep = float(parser.get("graphviz", "horizontal_sep"))
        ln.createGraphviz(targetTableName.upper(), templatePath, templateFileName,bgColor,fontName,srcSep,tgtSep ,filter)
        ln.writeGraphvizToPNG("{}/{}.png".format(outPath, targetTableName.upper()))
    elif graphType.lower() == "drawio":
            tableStyle = parser.get("drawio", "table_style")
            tableStyle = tableStyle[1:-1]
            columnStyle = parser.get("drawio", "column_style")
            columnStyle = columnStyle[1:-1]
            edgeStyle = parser.get("drawio", "edge_style")
            edgeStyle = edgeStyle[1:-1]
            stroColor = parser.get("drawio","stroke_color")
            sourceSpaceFactor = int(parser.get("drawio","source_space_factor"))
            sourceTargetSpaceFactor = int(parser.get("drawio", "source_target_space"))
            sourceTargetDist = int(parser.get("drawio", "srouce_target_distance"))
            itemHeight = int(parser.get("drawio", "item_height"))
            isInter = parser.get("drawio", "is_interactive")
            isInteractive = False
            strokColor = "#f51919"
            if isInter and isInter.lower() == "true":
                isInteractive = True
                if stroColor and len(strokColor) > 0:
                    strokColor = stroColor
            ln.generateDrawIOXMLLayout(targetTableName.upper(), tableStyle, columnStyle, edgeStyle,
                                       outPath, targetTableName.upper() + ".drawio", sourceSpaceFactor,
                                       sourceTargetSpaceFactor, sourceTargetDist, itemHeight, isInteractive,
                                       strokColor, filter)
    """
    argParser.add_argument("-p", "--sqlpath", help="Path of the SQL scripts")
    argParser.add_argument("-d", "--ddlpath", help="Path of the DDL scripts")
    argParser.add_argument("-t", "--table", help="Table name as target for the lineage")
    argParser.add_argument("-f", "--filter", help="Filter the intermediate tables [Y/N]")
    argParser.add_argument("-b", "--database", help="Database names for the intermediate tables to be filtered(comma delimted)")
    argParser.add_argument("-g", "--graph", help="Graph type[g=Graphviz,d=DrawIO]")
    argParser.add_argument("-s", "--stylespath", help="Styles file path for style.ini (table,column,edge,width,height) used by drawio")
    argParser.add_argument("-o", "--outpath", help="Output path")
    argParser.add_argument("-c", "--space", help="Space between the source tables")
    argParser.add_argument("-e", "--sourcetarget", help="space source to target factor")
    argParser.add_argument("-u", "--distance", help="source to target space to be multiplied by srctgt factor")
    argParser.add_argument("-i", "--height", help="item height")
    argParser.add_argument("-r", "--interactive", help="Add interactivity [Y/N]")
    argParser.add_argument("-k", "--stroke", help="stroke color")
    args = argParser.parse_args()

    ln = QueryLineageAnalysis(args.sqlpath, args.ddlpath, "./")
    ln.getLineage(args.table)
    filter = False
    if args.filter and args.filter.upper() == "Y":
        filter = True
        filtedDBs = args.database.split(",")
        ln.createfilteredRelations(args.table,filtedDBs)
    if args.graph:
        if args.graph.lower() == "g":
            ln.createGraphviz(args.table,"./",None,filter)
            ln.writeGraphvizToPNG("{}/{}.png".format(args.outpath,args.table))
        elif args.graph.lower() == "d":
            parser = SafeConfigParser() ### parse style.ini
            parser.read("{}/{}".format(args.stylespath,"style.ini"))
            tableStyle = parser.get("style","table_style")
            columnStyle = parser.get("style", "column_style")
            edgeStyle = parser.get("style", "edge_style")
            isInteractive = False
            strokColor = "#f51919"
            if args.interactive.upper() == 'Y':
                isInteractive = True
                if args.stroke:
                    strokColor = args.stroke
            ln.generateDrawIOXMLLayout(args.table, tableStyle, columnStyle, edgeStyle,
                                       args.outpath, args.table + ".drawio", int(args.space),
                                       int(args.sourcetarget), int(args.distance), int(args.height),isInteractive,
                                       strokColor,filter)
    """








