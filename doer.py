import pandas as pd 
import sqlite3
import os
connTempSQL=sqlite3.connect('TempSQL.sql')
df=pd.read_csv('edges.csv',names=['person','following'])
df.to_sql('DataSetInput',connTempSQL)
currTempSQL=connTempSQL.cursor()
currTempSQL.execute('SELECT person,following from DataSetInput')
tupples=[]
for x in currTempSQL:
	tupples.append(x)
connSQLOutput=sqlite3.connect('processedData.sql')
currSQLOutput=connSQLOutput.cursor()
currSQLOutput.execute('CREATE TABLE DataSet (person INTEGER, following INTEGER)')
for tupple in tupples:
	currTempSQL.execute('SELECT person,following from DataSetInput WHERE person=? and following=? and EXISTS (SELECT * from DataSetInput WHERE person=? and following=?)',(tupple[0],tupple[1],tupple[1],tupple[0]))
	for y in currTempSQL:
		currSQLOutput.execute('INSERT INTO DataSet (person,following) VALUES (?,?)',y)
connTempSQL.commit()
currTempSQL.close()
connTempSQL.close()
connSQLOutput.commit()
query='SELECT * from DataSet'
data=pd.read_sql(query,connSQLOutput)
data.set_index('person',inplace=True)
data.to_csv('processedData.csv',columns=['following'])
currSQLOutput.close()
connSQLOutput.commit()
connSQLOutput.close()
