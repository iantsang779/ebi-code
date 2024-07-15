## **matrix_parser.py**

Code used to transform the rice pangene matrix into a format usable for upload.py.
Transformed matrix has a unique row for each pangene ID, with it's corresponding gene ID for each of the 16 rice cultivars.


## **upload.py**

Code used to generate xref IDs and upload xref IDs/pangene IDs for the 16 core rice cultivars in the Pan Oryza project (plus 1 otherfeatures database for Nipponbare (LOC_genes)).

Usage:
    python upload.py --host {{database_name}} -u {{username}} -p {{port_number}} -pw {{password}}

Script Dependencies:
- argparse (https://pypi.org/project/argparse/)
- mysql.connector (https://pypi.org/project/mysql-connector-python/)
- pandas (https://pypi.org/project/pandas/)


## **rapid_release_fixes.py**

Code used to fix datacheck issues in plants-production-1 rice schemas for rapid release 65 handover.
