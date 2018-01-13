import sys
import os
from Queue import Queue
from threading import Thread
from subprocess import Popen,PIPE
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
import ConfigParser
import smtplib
import time
import xmlrpclib
import smtplib
import gzip
import socket

class Backup_Item:
      def __init__(self, lvName='', bPath='', index=0, tid=0):
          self.lv_name = lvName
          self.path    = bPath 
          self.index   = index
          self.taskid  = tid
      
# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
      rpc_paths = ('/RPC2',)

class BackupAgent:
      def __init__(self):
          self.config     = ConfigParser.ConfigParser()
          self.task_queue = Queue()
          self.pthread    = None
   
      def start(self):
          try:
              self.config.read('Backup.conf')
          except Exception, e:
              print 'Read config failed: %s' % e
              sys.exit()
     
          self.pthread        = Thread(target=self._backup)
          self.pthread.daemon = True
          self.pthread.start()

      def transferData(self, bItem):
	  try:
              ifName = bItem.lv_name+'.'+str(bItem.index)+'.img'
	      f_in = open(os.getcwd()+'/'+ifName, 'rb')
	      f_out = gzip.open(os.getcwd()+'/'+ifName+'.gz', 'wb')
	      f_out.writelines(f_in)
	      f_out.close()
	      f_in.close()          
	  except Exception, e:
              print 'compress file:%s failed, %s' % (ifName, e,)
              self._Alarm(self.config.get('Settings', 'sender'),
                          self.config.get('Settings', 'receiver'), 
                          'compress file failed: %s' % ifName )

          #scpCmd = 'sshpass -p '+self.config.get('Settings', 'password') + ' scp ' + \
          #          os.getcwd()+'/'+ifName + '.gz ' + self.config.get('Settings', 'user') + '@' + bItem.bPath
         
          # For testing only
          scpCmd = 'cp '+os.getcwd()+'/'+ifName+'.gz ' + self.config.get('Settings', 'datadir')+'/'+ifName+'.gz'
          print 'scp command: %s' % scpCmd

          scpProcess = Popen(scpCmd, shell=True)
          scpProcess.wait()
          if scpProcess.returncode != 0:
             if os.path.exists(os.getcwd()+'/'+ifName+'.gz'):
                os.remove(os.getcwd()+'/'+ifName+'.gz')
             return False
          
          if os.path.exists(os.getcwd()+'/'+ifName+'.gz'):
             os.remove(os.getcwd()+'/'+ifName+'.gz')
          return True

      def _backup(self):
          f = Popen(self.config.get('Settings', 'lv_search_cmd'), shell=True,stdout=PIPE).stdout
          lv_list = [ eachLine.strip() for eachLine in f ]
          f.close()

          server_proxy = xmlrpclib.ServerProxy('http://%s:%s' % 
                                               (self.config.get('Settings', 'manager_ip'),
                                                self.config.get('Settings', 'manager_port'), 
                                                ) )
          for lv in lv_list:
              bEof  = False
              index = 0
              while bEof == False:
                    if self.task_queue.empty():
                       try:
                           s = lv.split('/')
                           if len(s) != 0:
                              fName     = s[len(s)-1]+'.'+str(index)+'.img'
                              ddCmd     = 'dd if='+lv+' skip='+str(index)+' bs=20M count=5 of='+fName
                              ddProcess = Popen(ddCmd, shell=True)
                              ddProcess.wait()
                              if ddProcess.returncode != 0:
                                 print 'dd command failed: LV:%s;Block:%s' % (lv, str(index), )
                                 self._Alarm(self.config.get('Settings', 'sender'),
                                             self.config.get('Settings', 'receiver'), 
                                             'dd command failed: LV:%s;Block:%s' % (lv, str(index), ) )
                                 continue

                              fSize = os.stat(os.getcwd()+'/'+fName).st_size 
                              if fSize > 0:
                                 md5 = self._generateMd5(fName)
                                 server_proxy.request_backup(s[len(s)-1]+'.'+str(index), md5)
                                 print 'send md5: %s; file name:%s' % (md5, fName, )
                              
                              if fSize < 100*1024*1024:
                                 bEof = True
                       
                       except socket.error, e:
                           self._Alarm(self.config.get('Settings', 'sender'),
                                       self.config.get('Settings', 'receiver'), 
                                       'Request backup failed')
                       index = index + 1
                    else:
                       bItem = self.task_queue.get(False)
                       try:
                           if self.transferData(bItem):
                              server_proxy.notify(bItem.taskid, 1)
                           else:
                              server_proxy.notify(bItem.taskid, 0) 
                           print 'Finished, Notify Manager-----------------' 
                       except socket.error, e:
                           self._Alarm(self.config.get('Settings', 'sender'),
                                       self.config.get('Settings', 'receiver'), 
                                       'Notify Manager failed, taskid: %s' % bItem.taskid)
                           print 'Notify Manager failed-----------------' 
                        
                       if os.path.exists(os.getcwd()+'/'+fName):
                          os.remove(os.getcwd()+'/'+fName) 

      def notify(self, nDict):
          print 'Manager notify --------'
          try:
               if nDict['result'] != '0':
                  self._Alarm(self.config.get('Settings', 'sender'),
                              self.config.get('Settings', 'receiver'), 
                              'Invalid request has been sent')
               elif nDict['message'] == 'backup': 
                  print 'Manager reqeust to backup this data block'
                  bItem = Backup_Item() 
                  bItem.lv_name = nDict['block'].split('.')[0]
                  bItem.index   = nDict['block'].split('.')[1]
                  bItem.bPath   = nDict['path']
                  bItem.taskid  = nDict['taskid']
                  
                  self.task_queue.put_nowait(bItem)
               elif nDict['message'] == 'done':
                  print 'Same data exist, does not need to backup again'
                  if os.path.exists(os.getcwd()+'/'+nDict['block']+'.img'):
                     os.remove(os.getcwd()+'/'+nDict['block']+'.img')
               else:
                  pass 
 
          except Exception, e:    
                print e

          nMsg={}
          return nMsg
 
      def getFreeSpace(self):
          print 'in getFreeSpace----------------\n'
          sizeCmd = 'df -h|grep '+self.config.get('Settings', 'datadir') + '|awk \'{if(NF==6){print $4}else{print $3}}\''
          sizeProcess = Popen(sizeCmd, shell=True, stdout=PIPE)
          sizeProcess.wait()
          if sizeProcess.returncode != 0:
             self._Alarm(self.config.get('Settings', 'sender'),
                         self.config.get('Settings', 'receiver'), 
                         'Get free size failed: %s' % self.config.get('Settings', 'host'))
             return None
          
          for eachLine in sizeProcess.stdout:
              strOut = eachLine.strip()
              break;

          print 'Free space in this Agent is: '+strOut+'\n'

          strSize = strOut[:-1]
          unit    = strOut[-1:]
          
          if 'G' == unit:
              size = int(strSize)*1024
          elif 'M' == unit:
              size = int(strSize)
          else:
              size = 0

          return size

      def _Alarm(self, sender, receivers, message):
	  try:
	     smtpObj = smtplib.SMTP('localhost')
	     smtpObj.sendmail(sender, receivers, message)         
          except SMTPException:
             print "Error: unable to send email" 
      
      def _generateMd5(self, fName):
          md5Cmd = 'md5sum '+fName + '|awk \'{print $1}\''
          md5Process = Popen(md5Cmd, shell=True, stdout=PIPE)
          md5Process.wait()
          if md5Process.returncode != 0:
             self._Alarm(self.config.get('Settings', 'sender'),
                         self.config.get('Settings', 'receiver'), 
                         'Generate md5 failed: %s' % fName)
             return ''

          for each in md5Process.stdout:
              return each

          #try:
          #    f = open(fName, 'rb')
          #except IOError, e:
          #    return ''
          #try:
          #    d = hashlib.md5()
          #    buf = f.read(128)
          #    while '' != buf:
          #        d.update(buf)
          #        buf = f.read(128)
          #finally:
          #    f.close()
          #return d.hexdigest()         

bAgent = BackupAgent()
bAgent.start()

# Create server
server = SimpleXMLRPCServer((bAgent.config.get('Settings', 'host'), 
                             int(bAgent.config.get('Settings', 'port'))), 
                             requestHandler=RequestHandler)
server.register_introspection_functions()

# RPC methods available for Manager
server.register_function(bAgent.notify, 'notify')
server.register_function(bAgent.getFreeSpace, 'getFreeSpace')

# Run the server's main loop
server.serve_forever()

