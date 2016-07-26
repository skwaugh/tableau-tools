# created by Sawyer Waugh
# github = skwaugh
# July 26, 2016
#!/usr/in/python
from tableausdk import *
from tableausdk.Extract import *
import MySQLdb as sql
import pyodbc
import os
import time
import argparse


no_to_mysql_map = {
    0: 'DECIMAL', 1: 'TINY', 2: 'SHORT', 3: 'LONG',
    4: 'FLOAT', 5: 'DOUBLE', 6: 'NULL', 7: 'TIMESTAMP', 
    8: 'LONGLONG', 9: 'INT24', 10: 'DATE', 11: 'TIME',
    12: 'DATETIME', 13: 'YEAR', 14: 'NEWDATE', 15: 'VARCHAR',
    16: 'BIT', 246: 'NEWDECIMAL', 247: 'INTERVAL', 248: 'SET',
    249: 'TINY_BLOB', 250: 'MEDIUM_BLOB', 251: 'LONG_BLOB', 252: 'BLOB',
    253: 'VAR_STRING', 254: 'STRING', 255: 'GEOMETRY' 
}


mysql_to_tde_map = {
    "DATE": Type.DATE, "DATETIME": Type.DATETIME, 
    "DECIMAL": Type.DOUBLE, "FLOAT": Type.DOUBLE, "NEWDECIMAL": Type.DOUBLE,
    "INTEGER": Type.INTEGER,
    "VARCHAR": Type.CHAR_STRING, "CHAR": Type.CHAR_STRING, "VAR_STRING": Type.CHAR_STRING,
    "TEXT": Type.CHAR_STRING, "BLOB": Type.CHAR_STRING
}

tde_set_type_function_map = {
    Type.DATE: Row.setDate,
    Type.DATETIME: Row.setDateTime,
    Type.DOUBLE: Row.setDouble,
    Type.CHAR_STRING: Row.setCharString,
    Type.INTEGER: Row.setInteger
}



def printTableDefinition(tableDef):
    for i in range(tableDef.getColumnCount()):
        field_type = tableDef.getColumnType(i)
        field = tableDef.getColumnName(i)
        print "Column {0}: {1} ({2})".format(i, field, field_type)



def getODBCCursor():
    try:
        cnxn_str = "DSN=NetSuite;UID=swaugh@luxbp.com;PWD=Luxury2015"
        cnxn = pyodbc.connect(cnxn_str)
        cursor = cnxn.cursor()
        return cursor, cnxn
    except Exception as e:
        print "getODBCCursor failed"



def makeTableDefinition(fields):
    table_def = TableDefinition()
    table_def.setDefaultCollation(Collation.EN_GB)
    c = []
    for field in fields:
        mysql_type = no_to_mysql_map[field[1]]
        field_type = mysql_to_tde_map.get(mysql_type, Type.CHAR_STRING)
        #print mysql_type + ": " + str(field[1]) + " | " + str(field_type)
        table_def.addColumn(field[0], field_type)
        c.append(field_type)

    return table_def

#@profile
def createTDE(query, dbname, tde_filename):
    start_time = time.time()
    db = sql.connect(host="127.0.0.1", port=9999, user="root", db=dbname)
    cursor = db.cursor()
    cursor.execute(query)
    
    ExtractAPI.initialize()

    if os.path.isfile(tde_filename):
        os.remove(tde_filename)
    tde = Extract(tde_filename)
    
    table_def = makeTableDefinition(cursor.description)
    
    tde_table = tde.addTable('Extract', table_def)

    tde_row = Row(table_def)   # Pass the table definition to the constructor

    size = 1000
    many = cursor.fetchmany(size)
    rows_counter = 0
    while many:
        # add counter for rows incase of database disconnection
        for row in many:
            # Create new row
            for colno, col in enumerate(row):

                col_type = table_def.getColumnType(colno)
                setType = tde_set_type_function_map[col_type]
                # print setType
                # print type(setType)
                setType(tde_row, colno, col)
            tde_table.insert(tde_row)
            rows_counter += 1
        many = cursor.fetchmany(size)
            #tde_row.close()
    print "cleared while loop"
            # Set column values. The first parameter is the column number (its
            # ordinal position) The second parameter (or second and subsequent paramaters) is 
            # the value to set the column to.    
                
#                 new_row.setInteger(0, 1)
#                 new_row.setString(1, 'Amanda')
#                 new_row.setDate(2, 2014, 6, 9)
#                 new_table.insert(new_row) # Add the new row to the table


# new_row.setInteger(0, 1)
# new_row.setString(1, 'Amanda')
# new_row.setDate(2, 2014, 6, 9)
# new_table.insert(new_row) # Add the new row to the table

# new_row.setInteger(0, 2)
# new_row.setString(1, 'Brian')
# new_row.setDate(2, 2014, 10, 13)
# new_table.insert(new_row)

# new_row.setInteger(0, 3)
# new_row.setString(1, 'Christina')
# new_row.setDate(2, 2015, 2, 16)
# new_table.insert(new_row)

    # Close the extract in order to save the .tde file and clean up resources
    tde.close()
    print "TDE Closed"
    ExtractAPI.cleanup()
    print "ExtractAPI cleaned up"
    cursor.close()
    print "cursor Closed"
    db.close()
    print "db closed"

    timetaken = time.time() - start_time
    print str(rows_counter) + ' rows inserted in ' + str(timetaken) + ' seconds'
    print '    (' + str(rows_counter/timetaken) + ' rows per second)'
    return


def main():

    parser = argparse.ArgumentParser(description='ADD YOUR DESCRIPTION HERE')
    parser.add_argument('-q','--query', help='SQL Query', required=True)
    parser.add_argument('-d','--database', help='Database Name', required=True)
    parser.add_argument('-o','--outfile', help='.tde file for output', required=True)
    args = parser.parse_args()

    print str(args.database)
    print str(args.query)

    try:
        createTDE(args.query, args.database, args.outfile)
        print "successfully wrote Tableau Data Extract to " + str(args.outfile)
    except Exception as e:
        print "Failed to create file with error: " + str(e)

if __name__ == '__main__':
    main()

