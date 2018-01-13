import sys
import sqlite3

class dbException(Exception):
      def __init__(self, arg):
          self.msg = arg

class DBEngine:
      def __init__(self, dbfname):
          self.dbfname = dbfname

      def __del__(self):
          self.conn.close()

      def initDB(self, sqlFile, sqlClause=None):
          if sqlFile is None and sqlClause is None:
             raise dbException('Either SQL file name or SQL clause is needed!')
          else:
	      self.conn = sqlite3.connect(self.dbfname)
	      self.cur  = self.conn.cursor()
	     
	      if sqlFile is not None:
                 try:
		     initSql = open(sqlFile, 'r').read()
                 except Exception:
                     raise dbException('Read sql file failed')
	      else:
		 initSql = sqlClause

	      try:
		  self.cur.execute(initSql)
		  self.conn.commit()
	      except Exception, e:
		  raise dbException('Sql syntax error: %s' % e)

      def selectRecords(self, tName, fList=None, cDict=None, limit=None):
          if tName is None:
             raise dbException('Please provide name of the table to search')

          selectSql = 'select '
          if fList is not None and len(fList) != 0:
             for field in fList:
                 selectSql += field
                 selectSql += ',' 
             selectSql = selectSql[:-1] 
          else:
             selectSql += '*'
          selectSql += ' from %s' % tName

          if cDict is not None and len(cDict) != 0:
              selectSql += ' where '
              rKeys = cDict.keys()
	      for rKey in rKeys:
	          selectSql += rKey
	          selectSql += '='
	          selectSql += cDict[rKey]
	          selectSql += ' and '
              selectSql = selectSql[:-5]
      
          if limit is not None:
             selectSql += ' limit %d' % limit

          print 'sql: %s' % selectSql
           
          try:
              self.cur.execute(selectSql)
              rows       = self.cur.fetchall()
              selRecords = []
              for row in rows:
                  singleRecord = []
                  for cell in row:
                      singleRecord.append(cell)
                  selRecords.append(singleRecord)
              return selRecords                    
          except Exception, e:
              raise dbException('Select record from \'%s\'failed: %s' % (tName, e, ) )

      def addRecord(self, tName, rDict):
          if tName is None:
             raise dbException('Please provide name of the table to add record')
          if rDict is None or len(rDict) == 0:
             raise dbException('Empty record is not permitted')
             
          insertSql = 'insert into %s(' % tName
          
          rKeys = rDict.keys()
          
          for rKey in rKeys:
              insertSql += rKey
              insertSql += ','
          
          insertSql  = insertSql[:-1]
          insertSql += ') values('  
      
          for rKey in rKeys:
              insertSql += rDict[rKey]
              insertSql += ','

          insertSql  = insertSql[:-1]
          insertSql += ');'       

          print 'sql: %s' % insertSql
          
          try:
             self.cur.execute(insertSql)
             self.conn.commit()
             return self.cur.lastrowid
          except Exception, e:
              raise dbException('Add record to table \'%s\' failed: %s' % (tName, e,))

      def updateRecords(self, tName, fDict, cDict):
          if tName is None:
             raise dbException('Please provide name of the table to update')
          if len(fDict) == 0:
             raise dbException('No field to update')
          
          updateSql = 'update %s set ' % tName
          
          rKeys = fDict.keys()
          for rKey in rKeys:
              updateSql += rKey
              updateSql += '='
              updateSql += fDict[rKey]
              updateSql += ','

          updateSql = updateSql[:-1] 
       
          if cDict is not None and len(cDict) != 0:
             updateSql += ' where '
             rKeys      = cDict.keys()
             for rKey in rKeys:
                 updateSql += rKey
                 updateSql += '='
                 updateSql += cDict[rKey]
                 updateSql += ' and '
             updateSql = updateSql[:-5]

          print 'sql: %s' % updateSql
          
          try:
             self.cur.execute(updateSql)
             self.conn.commit()
          except Exception, e: 
             raise dbException('update table \'%s\' field failed: %s' % (tName, e,) ) 
      
      def delRecords(self, tName, cDict):       
          if tName is None:
             raise dbException('Please provide name of the table to delete record')
          if cDict is None or len(cDict) == 0:
             raise dbException('Condition must provided for deleting')
          
          deleteSql = 'delete from  %s' % tName
          deleteSql += ' where '
          
          rKeys = cDict.keys()
	  for rKey in rKeys:
	      deleteSql += rKey
	      deleteSql += '='
	      deleteSql += cDict[rKey]
	      deleteSql += ' and '
          deleteSql = deleteSql[:-5]

          print 'sql: %s' % deleteSql
          
          try:
              self.cur.execute(deleteSql)
              self.conn.commit()
          except Exception, e:
              raise dbException('Delete record from \'%s\' failed: %s' % (tName, e,) )
