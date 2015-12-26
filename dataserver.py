__author__ = 'Chirag'
from pymongo import MongoClient
import logging
import xmlrpclib
import sys, SimpleXMLRPCServer,getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary
from threading import Thread
import os

class RemoteServer:
  def __init__(self):
    self.data = {}
    self.next_check = datetime.now() + timedelta(minutes = 5)
 

  # Insert something into the Dataserver
  def put(self, key, value):
    # Remove expired entries
    print("Key to be entered:",key.data)
    print("Value given",value.data)
    key_update = {'_id':key.data}
    value_update  ={'$set':{'val':value.data,"length":len(value.data)}}
    test = m_table.update_one(key_update,value_update,True)
    print("Insert done!!",test)
    return True

   # Retrieve something from the Dataserver
  def get(self, key):
      rv = {}# Default return value
      # Loop to traverse through MongoDB and retrieve the row corresponding to 'key'
      print("Key entered:",key.data)
      val_obj = m_table.find_one({"_id": key.data})
      if val_obj == None:
           print None
      else:
           print("Value of object",val_obj['val'])
           rv={"value": Binary(val_obj['val'])}
      return rv
  # Method to emulate pinging
  def ping(self,data):
       return data
  
  # Method to get the data length stored in the DB
  def getchecksum(self,key):
        # Loop to traverse through MongoDB and retrieve the row corresponding to 'key'
        val_obj = m_table.find_one({"_id": key.data})
        if val_obj == None:
           print None
           return None
        else:
           print("Object value",val_obj['length'])
           return val_obj['length'] 

  # Method to retrieve all the rows of the server
  def sync(self):
      data=[]
      for value in m_table.find():
          data.append(value)
      print "All values returned!!"
      print data
      return pickle.dumps(data)
  
  # Method to corrupt the data in the server by changing the length of the data
  def corrupt(self,key):
      table_key=key.data+"&&data"
      val_obj= m_table.find_one({"_id":key.data})
      if val_obj==None:
         return None
      else:
         test= val_obj['length']
         key_update = {'_id':key.data}
    	 value_update  ={'$set':{'val':val_obj['val'],"length":test+1}}
    	 test = m_table.update_one(key_update,value_update,True)
         print test

  # Method to terminate the server through python scripts
  def terminate(self):
      os.system("kill -9 %d"%(os.getppid()))

  # Method to return the list of keys stored in the DB
  def list_contents(self):
      data=[]
      for table_key in m_table.find():
          data.append(table_key['_id'])
      return Binary(pickle.dumps(data))

def main():
  port=int(sys.argv[2])		#Variable to store the server port
  serve(port)			# Execution of the method to initialize and start the server
  
# Start the xmlrpc server
def serve(port):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port),allow_none=True)
  file_server.register_introspection_functions()
  sht = RemoteServer()
  # Registering all the interfaces of the server
  file_server.register_function(sht.get)
  file_server.register_function(sht.put)
  file_server.register_function(sht.getchecksum)
  file_server.register_function(sht.ping)
  file_server.register_function(sht.sync)
  file_server.register_function(sht.corrupt)
  file_server.register_function(sht.terminate)
  file_server.register_function(sht.list_contents)
  file_server.serve_forever()

if __name__ == '__main__':
    metadataurl = sys.argv[1]
    # Initializing the DB instance of the Server using the URL mentioned in the command line
    metaDataDB = MongoClient(metadataurl)
    metadsn = metaDataDB.meta_table
    m_table = metadsn.metadata_collection
    
    main()
    

