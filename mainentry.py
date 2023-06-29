import argparse
from configparser import SafeConfigParser
from querylineage2 import QueryLineageAnalysis

if __name__ == "__main__":
    """
    Do all generation in one go and process
    and call the drawio to export to html
    """
    argParser = argparse.ArgumentParser()
    argParser.add_argument("-p", "--sqlpath", help="Path of the SQL scripts")
    argParser.add_argument("-d", "--ddlpath", help="Path of the DDL scripts")
    argParser.add_argument("-t", "--table", help="Table name as target for the lineage")
    argParser.add_argument("-f", "--filter", help="Filter the intermediate tables [Y/N]")
    argParser.add_argument("-b", "--databse", help="Database names for the intermediate tables to be filtered(comma delimted)")
    argParser.add_argument("-g", "--graph", help="Graph type[g=Graphviz,d=DrawIO]")
    argParser.add_argument("-s", "--stylespath", help="Styles file path for style.ini (table,column,edge,width,height) used by drawio")
    argParser.add_argument("-o", "--outpath", help="Output path")
    argParser.add_argument("-c", "--space", help="Space between the source tables")
    argParser.add_argument("-e", "--sourcetarget", help="space source to target factor")
    argParser.add_argument("-h", "--distance", help="source to target space to be multiplied by srctgt factor")
    argParser.add_argument("-i", "--height", help="item height")
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
            ln.generateDrawIOXMLLayout(args.table,tableStyle,columnStyle,edgeStyle,
                                       args.outpath,args.table+".drawio",int(args.space),
                                       int(args.sourcetarget),int(args.distance),int(args.height),filter)







    print("args.name=%s" % args.ddlpath)