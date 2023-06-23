import time 
import sys
import stomp

class Listener(stomp.ConnectionListener):
    def on_error(self, headers, message):
        print('received an error "%s"' % message)
    def on_message(self, headers, message):
        print('received a message "%s"' % message)

class activeMqNode:
    def __init__(self, hostPort, id, queryTopic, inputTopics): 
        hosts = [('localhost', hostPort)] 
        conn = stomp.Connection(host_and_ports=hosts)
        conn.set_listener(id, Listener())
        #conn.start()
        conn.connect('admin', 'admin', wait=True)

        for topic in inputTopics:
            conn.subscribe(destination='/topic/'+topic, id = queryTopic+"_"+id, ack='auto') 

        self.conn = conn
        self.id = id
        self.queryTopic = queryTopic
        self.inputTopics = inputTopics
    
    def send(self, message):
        self.conn.send(body=message, destination='/topic/'+self.queryTopic)
    
    def disconnect(self):
        self.conn.disconnect()

    def __str__(self):
        return f"Connection(conn={self.conn}, id='{self.id}', queryTopic='{self.queryTopic}', inputTopics={self.inputTopics})"
    


    
    
    

    