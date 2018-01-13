import sys
import os
from Queue import Queue
from threading import Thread
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
import ConfigParser
import smtplib
import time
import xmlrpclib
import socket
import hashlib
from functools import partial
from BackupDb import *

SQLITE_THREADSAFE=1

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
      rpc_paths = ('/RPC2',)
      
      def do_POST(self):
        global bManager 
        bManager.client_ip, bManager.client_port = self.client_address
        SimpleXMLRPCRequestHandler.do_POST(self) 

class Task_Item:
      def __init__(self):
          client_ip = ''
          client_port = 0 
          block_name = md5 = ''

def dump_task(tItem):
    print '\n' 
    print 'client ip: '+tItem.client_ip+'\n'
    print 'client port: '+str(tItem.client_port)+'\n'
    print 'block_name: '+tItem.block_name+'\n'
    print 'md5: '+tItem.md5+'\n'

# Backup Manager
class BackupMgr:
      def __init__(self):
          self.config      = ConfigParser.ConfigParser(allow_no_value=True)
          self.db          = None
          self.task_queue  = Queue()
          self.pthread     = None
          self.bWait       = False
          self.client_ip   = None
          self.client_port = None

      def start(self):
          try:
              self.config.read('Backup.conf')
          except Exception, e:
              print 'Read Backup.conf error: %s' % e 
              sys.exit()

          self.pthread        = Thread(target=self._process_backup)
          self.pthread.daemon = True
          self.pthread.start()

      def request_backup(self, block_name, md5):
          
          retMsg = {'result': '1', 'message':'invaid', 'path':''}
          
          if block_name is None or md5 is None:
             return retMsg
          elif len(block_name) == 0 or len(md5) == 0:
             return retMsg

          tItem            = Task_Item()
          tItem.client_ip  = self.client_ip
          tItem.client_port= self.client_port
          tItem.md5        = md5
          tItem.block_name = block_name
          
          self.task_queue.put_nowait(tItem) 
          
          retMsg['result']  = '0'
          retMsg['message'] = 'wait'
         
          print 'Request from client:\n' 
          dump_task(tItem)
 
          return retMsg         

      def _process_backup(self):
          
          try:
             self.db = DBEngine(self.config.get('Settings', 'dbfile'))
             self.db.initDB(self.config.get('Settings', 'sqlfile'))
          except dbException, e:
             print e.msg
             sys.exit()

          nRetry = 0 
          while True:
              # There is one backup in progress... 
              if self.bWait is True:
                 if nRetry < 10:
                    time.sleep(2)
                    nRetry = nRetry+1
                    continue
              
              retry = 0

              try:
	          tItem = self.task_queue.get() 
              except Exception, e:
                  print 'Empty queue: %s' % e
                  continue                  
              
              client_proxy = xmlrpclib.ServerProxy('http://%s:%s' % ( tItem.client_ip, 
                                                    self.config.get('Settings', 'agent_port'), ) )

              block_name = tItem.block_name
	      md5        = tItem.md5

              if len(block_name) == 0 or len(md5) == 0:
                 print 'Invalid task item'
                 continue

	      fList = ['id', 'md5', 'data_path']
	      cDict = {'lv_block':'\''+block_name+'\''}
              try:
	          rList = self.db.selectRecords('backup_record', fList, cDict)
              except dbException, e:
                  print e.msg
                  self._Alarm(self.config.get('Settings', 'sender'),
                              self.config.get('Settings', 'receiver'), e.msg)
                  continue
              
              print 'Handling data--------------\n'

	      # If this data block has not been backed up
	      if len(rList) == 0:
                 print 'This block has not been backed up\n'
		 fList = ['data_path']
		 cDict = {'md5':'\''+md5+'\''}
		 limit = 1
                 try:
		     rList = self.db.selectRecords('backup_record', fList, cDict, limit)
                 except dbException, e:
                     self._Alarm(self.config.get('Settings', 'sender'),
                                 self.config.get('Settings', 'receiver'), e.msg)
                     continue
		 
                 # No existing data block has the same data as this one
		 if len(rList) == 0:
                    print 'No existing data block has the same data as this one\n'
		    data_path = self._getDataPath() 
		    rDict={'id':'NULL', 'lv_block':'\''+block_name+'\'', 'md5':'\''+md5+'\'', 'state':'0','data_path':'\''+data_path+'\''}  
                    try:
                        taskid = self.db.addRecord('backup_record', rDict) 
                    except dbException, e:
                        print e
                        self._Alarm(self.config.get('Settings', 'sender'),
                                    self.config.get('Settings', 'receiver'), e.msg)
                        continue

                    nMsg={}
		    nMsg['result']  = '0'
		    nMsg['message'] = 'backup'
                    nMsg['block']   = block_name
		    nMsg['path']    = data_path
		    nMsg['taskid']  = taskid

                    try: 
                        client_proxy.notify(nMsg)
                    except Exception, e1:
                        print e1
                    except socket.error, e2:
                        self._Alarm(self.config.get('Settings', 'sender'),
                                    self.config.get('Settings', 'receiver'), 
                                    'Fail to call notify() on agent: %s:%s'%(tItem.client_ip,tItem.client_port,))
                        continue

                    self.bWait = True
                 
                 else: # Same data block found, no bother about doing that again
                    print 'Same data block found, reference it' 
		    rDict = {'id':'NULL', 'lv_block':'\''+block_name+'\'', 
                             'md5':'\''+md5+'\'', 'state':'1', 'data_path':'\''+rList[0][0]+'\''}  
                    try:
		        taskid = self.db.addRecord('backup_record', rDict)
                    except dbException, e:
                        self._Alarm(self.config.get('Settings', 'sender'),
                                    self.config.get('Settings', 'receiver'), e.msg)
                        continue

		    nMsg['result']  = '0'
		    nMsg['message'] = 'done'
                    nMsg['block']   = block_name
		    nMsg['path']    = data_path
		    nMsg['taskid']  = taskid
		    
                    try: 
                        client_proxy.notify(nMsg)
                    except Exception, e1:
                        print e1
                    except socket.error, e2:
                        self._Alarm(self.config.get('Settings', 'sender'),
                                    self.config.get('Settings', 'receiver'), 
                                    'Fail to call notify() on agent: %s:%s'%(tItem.client_ip,tItem.client_port,))
                        continue

	      else:
		  taskid      = rList[0][0]
		  oldMd5      = rList[0][1]
		  oldDataPath = rList[0][2]
		  
		  # Data has been modified
		  if md5 != oldMd5:
                     print 'This block has been modified\n'
		     fList = ['data_path']
		     cDict = {'md5':'\''+md5+'\''}
		     limit = 1
                     try:
		         rList = self.db.selectRecords('backup_record', fList, cDict, limit)
                     except dbException, e:
                         self._Alarm(self.config.get('Settings', 'sender'),
                                     self.config.get('Settings', 'receiver'), e.msg)
                         continue
		     
                     # Check if there is data the same as this modified one
		     if len(rList) == 0:
			# Overwriting backup
                        print 'No block is the same as this modified one\n'
			fDict = {'state': 0}
			cDict = {'id': taskid}
                      
                        try:
			    self.db.updateRecords('backup_record', fDict, cDict) 
                        except dbException, e:
                            self._Alarm(self.config.get('Settings', 'sender'),
                                        self.config.get('Settings', 'receiver'), e.msg)
                            continue
		
                        nMsg={}	
                        nMsg['result']  = '0'
			nMsg['message'] = 'backup'
                        nMsg['block']   = block_name
			nMsg['path']    = oldDataPath
			nMsg['taskid']  = taskid
	
                        try:	
                            client_proxy.notify(nMsg)
                        except Exception, e1:
                            print e1
                        except socket.error, e2:
                            self._Alarm(self.config.get('Settings', 'sender'),
                                        self.config.get('Settings', 'receiver'), 
                                        'Fail to call notify() on agent: %s:%s'%(tItem.client_ip,tItem.client_port,))
                            continue
                
                        self.bWait = True
                        
		     else: # Update data path
                        print 'Update data path, does not need to backup this new one'
			refPath = rList[0][0]
			fDict   = {'data_path': '\''+refPath+'\''}
			cDict   = {'lv_block': '\''+block_name+'\''}
                   
                        try:
			    self.db.updateRecords('backup_record', fDict, cDict) 
                        except dbException, e:
                            self._Alarm(self.config.get('Settings', 'sender'),
                                        self.config.get('Settings', 'receiver'), e.msg)
                            continue
		     
                     # Update md5
                     print 'Update md5 value for data block\n'
		     fDict = {'md5': '\''+md5+'\''}
		     cDict = {'lv_block': '\''+block_name+'\''}

                     try:
		         self.db.updateRecords('backup_record', fDict, cDict) 
                     except dbException, e:
                         self._Alarm(self.config.get('Settings', 'sender'),
                                     self.config.get('Settings', 'receiver'), e.msg)
                         continue
		  
                  else: # Data has not been modified
                      print 'Data has not been modified\n' 
		     
                      nMsg={} 
                      nMsg['result']  = '0'
		      nMsg['message'] = 'done'
                      nMsg['block']   = '\''+block_name+'\''
		      nMsg['path']    = ''
                      nMsg['taskid']  = str(taskid)
		     
                      try: 
                          client_proxy.notify(nMsg)
                      except Exception, e1:
                          print e1
                      except socket.error, e2:
                          self._Alarm(self.config.get('Settings', 'sender'),
                                      self.config.get('Settings', 'receiver'), 
                                      'Fail to call notify() on agent: %s:%s'%(tItem.client_ip,tItem.client_port,))
                          continue


      def notify(self, taskId, state):
          print 'Notify from client, taskid: %s, state: %s' % (taskId, state, )	
          
          retMsg={'result':'1', 'message':'invalid'}
          
          if taskId < 0:
             return retMsg
          if state != '1' or state != '0':
             return retMsg

          if state != '1':
              print 'Backup failed, taskId: %s' % taskId
              self._Alarm(self.config.get('Settings', 'sender'),
                          self.config.get('Settings', 'receiver'),
                         'Backup task failed, taskId:%s' % taskId)
          else: 
              fDict = {'state': state}
              cDict = {'id', taskId}
              try:
                  self.db.updateRecords('backup_record', fDict, cDict)
              except dbException, e:
                  self._Alarm(self.config.get('Settings', 'sender'),
                              self.config.get('Settings', 'receiver'), e.msg)

          self.bWait        = False
          retMsg['result']  = '0'
          retMsg['message'] = 'done'
           
          return retMsg
       
      def _Alarm(self, sender, receivers, message):
	  print 'Send mail to administrator!'
          return
          try:
	     smtpObj = smtplib.SMTP(self.config.get('Settings', 'host'))
	     smtpObj.sendmail(sender, receivers, message)         
          except smtplib.SMTPException:
             print "Error: unable to send email" 

      def _getDataPath(self):
         print 'get path to store data\n'
         agents = self.config.get('Settings', 'agents')
         port   = self.config.get('Settings', 'agent_port')

         agentChosen = {'ip':'', 'free':0}         

         for agent in agents.split(';'):
             client_proxy = xmlrpclib.ServerProxy('http://%s:%s' % (agent, port,) )
             
             try:             
                 freeSpace = client_proxy.getFreeSpace()
                 print 'Free space in Agent: %s is: %sM' % (agent, str(freeSpace), )
             except Exception, e1:
                 print e1
                 continue
             except socket.error, e2:
                 self._Alarm(self.config.get('Settings', 'sender'),
                             self.config.get('Settings', 'receiver'), 
                             'Call getFreeSpace() on agent: %s:%s failed' % (agent, port,) )
                 continue
             
             if agentChosen['ip']   == '':
                agentChosen['ip']   = agent
                agentChosen['free'] = freeSpace
             else:
                if agentChosen['free'] < freeSpace:
                   agentChosen['ip']   = agent
                   agentChosen['free'] = freeSpace

         return agentChosen['ip'] + ':' + self.config.get('Settings', 'datadir') 
  
bManager = BackupMgr()
bManager.start()
   
# Create server
server = SimpleXMLRPCServer((bManager.config.get('Settings', 'host'), 
                             int(bManager.config.get('Settings', 'port'))), 
                             requestHandler=RequestHandler,
                             allow_none=True)
server.register_introspection_functions()

# RPC methods available for agent
server.register_function(bManager.request_backup, 'request_backup')
server.register_function(bManager.notify, 'notify')

# Run the server's main loop
server.serve_forever()



