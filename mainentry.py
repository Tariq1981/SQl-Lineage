import argparse

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
    argParser.add_argument("-b", "--databse", help="Database name for the intermediate tables to be filtered")
    argParser.add_argument("-g", "--graph", help="Graph type[g=Graphviz,d=DrawIO]")
    argParser.add_argument("-s", "--styles", help="Styles file path for style.ini (table,column,edge,width,height) used by drawio")
    argParser.add_argument("-o", "--outpath", help="Output path")
    args = argParser.parse_args()

    ln = QueryLineageAnalysis(args.sqlpath, args.ddlpath, "./")
    ln.getLineage(args.table)
    filter = False
    if args.filter and args.filter.upper() == "Y":
        filter = True
        ln.createfilteredRelations(args.table,args.database)
    if args.graph
        if args.graph.lower() == "g":
            ln.createGraphviz(args.table,"./",None,filter)
            ln.writeGraphvizToPNG("{}/{}.png".format(args.outpath,args.table))
        elif args.graph.lower() == "d":
            pass






    print("args.name=%s" % args.ddlpath)