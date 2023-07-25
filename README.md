## Purpose:
This tool can be used to extract the column lineage from ETL scripts.
It will draw the lineage wither using graphviz or DrawIO.
Drawing using DrawIO has an additional feature which makes the diagram to
be interactive by showing the lineage for particular column when it is clicked.

##Classes:
The following diagram demonstrates the classes developed for this tool:

![Screenshot of a comment on a GitHub issue showing an image, added in the Markdown, of an Octocat smiling and raising a tentacle.](./lineage_class_diagram.png)

The developed classes are:
- [QueryLineageAnalysis](./querylineage2.py):
This class is responsible for the following:
  * Reading and parsing the DDLs.
  * Reading and Parsing the SQLs which will be used in the getting the lineage.
  * Calling other classes to draw the returned lineage.
- [LineageDiagram](./lineage_diagram.py): 
This class is responsible for the following:
  * Creating the graphviz format for the diagram using dot library
  * Saving the diagram as png, svg or text format using dot.
- [LineageToDrawIO](./lineagetodrawio.py):
This class is responsible for the following:
  * Creating DrawIO file for the resulted lineage.
  * Adding interactivity on demand for the diagram.
  * Aligning the nodes in order to overlap.

##Input file description:
The input for this tool is an ini file similar to [lineage_config.ini](./lineage_config.ini)
Below are the 

##Prequisites:
Diagraming options:

##Files
Limitations:

##Sample SQL and Lineage diagrams creen shots

##Libraries