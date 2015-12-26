__author__ = 'vyas'
from pymongo import MongoClient
import xmlrpclib
import sys, SimpleXMLRPCServer,getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary
import socket
class LoadBalancer:
    
    def __init__(self,serverPorts,Qr=1,n=2):
       # Initializing MetaData  and Dataserver
       self.metaserver = xmlrpclib.ServerProxy("http://localhost:"+serverPorts[0],allow_none=True) 
       self.noOfReadServer=int(Qr)			# No of Read servers
       self.dataserver=[]				# List Dataservers
       self.serverStatus,self.sDetails={},{}
       # Initializing the meta and data server clients and variables to hold their status
       for i in range(int(n)):
	   server = xmlrpclib.ServerProxy("http://localhost:"+serverPorts[i+1],allow_none=True)
           self.dataserver.append(server)
           self.serverStatus.update({str(server):True}) # Initializing all the servers as up and running
           self.sDetails.update({str(server):server})
       self.noOfServer=int(n)				# No of Write Servers
       
    # Method to check if the servers are in running mode
    def check(self,serverdetails):
        try:
           returnVal = serverdetails.ping(pickle.dumps("OK")) 		   # Check if the remote servers are up
           if pickle.loads(returnVal) == "OK":
              print "Server is up and runnning"
              if self.serverStatus[str(serverdetails)]==False:		   # Check of the previous server status
                 print " Server updation comencing"
                 print self.serverStatus
                 temp_serverKey = self.serverStatus.keys()
                 # Loop to check present running server from where replication can be done
                 for key in temp_serverKey: 
                     print("The server is",key)
                     if self.serverStatus[key] == True:
                        self.syncServers(self.sDetails[key],serverdetails) # Method to sync backend DB of the servers
                 print"Server updation done!!"
              self.serverStatus[str(serverdetails)]=True               # Changing the server status from False to True
              return True
           else:
              print "Server not running or is slow"
              self.serverStatus[str(serverdetails)]=False      # Changing the server status from True to False
              return False
        except socket.error as err:
              self.serverStatus[str(serverdetails)]=False      # Changing the server status from True to False
              print self.serverStatus
              print serverdetails
              print "server down"
              return False
   
    # Method to insert data into the Data server
    def putdata(self,key,value):
        # Loop to insert values into all the available data server
        for i in range(self.noOfServer):
	    if (self.check(self.dataserver[i])== True):
               self.dataserver[i].put(key,value)
	       count=0
               # Loop to check if data write is correct, if there is an issue retry 5 times
	       while(self.dataserver[i].getchecksum(key)==None and count<5):
		    count+=1
		    self.dataserver[i].put(key,value)       # Method to insert values into the remote server
        return True
    
    # Method to get values from the Data server
    def getdata(self,key):
       count=0						# Counter to store no of correct read
       for i in range(self.noOfServer):
        if (self.check(self.dataserver[i])== True):     # Loop to check if the servers are runnning
         checkSum=self.dataserver[i].getchecksum(key)   # Variable to store the length of the data
         pickle_data=self.dataserver[i].get(key)        # Variable to store the data
         returnVal=pickle_data["value"]   		# Extracting the data   
         print "data obtained"
         if int(checkSum)==len(returnVal.data):     # Checking length of the data stored in DB with actual datalength
            count+=1
         else:
            self.serverStatus[str(self.dataserver[i])]=False  # Setting the server status as corrupted for sync
            print("Server Details:",self.serverStatus)
       if count>=self.noOfReadServer:			# Check if the required Quorum is met
	  return pickle_data
  
    # Method to insert data into the Metadata server
    def put(self,key,value):
         print key
         print value.data
         self.metaserver.put(key,value)
         print "value"
         return True
    #Method to get values from the Data server
    def get(self,key):
         return self.metaserver.get(key)

    # Method to synchronize the data
    def syncServers(self,fromServerDetails,toServerDetails):
        print "Ready for data transfer"
        # Getting the data from the remote server for synchronization
        data = pickle.loads(fromServerDetails.sync())
        print ("From server:",fromServerDetails)
        print ("To SERVER:",toServerDetails)
        # Loop to replicate the data from Non corrupt to corrupt server
        for temp in data:
            key=temp['_id']
            value=temp['val']
            print("key:",key)
            print("value",value)
            toServerDetails.put(Binary(key),Binary(pickle.dumps(value)))
        print"End of Synchronization"
                

def main():
  inputValues="Qr::"+sys.argv[2]+" Qw::"+sys.argv[3]+" Meta&Data server ports:"+str(sys.argv[4:])
  print("The following are the parameters entered:",inputValues)
  lbserve(int(sys.argv[1]),sys.argv[4:],sys.argv[2],sys.argv[3])
  

# Start the xmlrpc server
def lbserve(port,ports,Qr=1,Qw=2):
  lb_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port),allow_none=True)
  lb_server.register_introspection_functions()
  lb = LoadBalancer(ports,Qr,Qw)
  lb_server.register_function(lb.put)
  lb_server.register_function(lb.get)
  lb_server.register_function(lb.putdata)
  lb_server.register_function(lb.getdata)
  lb_server.serve_forever()

if __name__ == '__main__':
   main()
