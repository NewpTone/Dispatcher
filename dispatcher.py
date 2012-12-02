#!/usr/bin/python
# Auhtor:Yu xingchao
# Date: 2012.11.19


import subprocess
import ConfigParser
from threading import Thread
from Queue import Queue
import time
""""
A multi-thread cmd dispatch system

"""

start = time.time()
queue = Queue()

def readConfig(file="config.ini"):
    """Extract IP address and CMDS from config file and return tuple"""
    ip_pool = []
    cmd_pool = []
    Config=ConfigParser.ConfigParser()
    Config.read(file)
    machines = Config.items("MACHINES")
    commands = Config.items("COMMANDS")
    for ip in machines:
        ip_pool.append(ip[1])
    for cmd in commands:
        cmd_pool.append(cmd[1])
        print cmd[1]
    return ip_pool,cmd_pool

def launcher(i,q,cmd):
    """Spawns command in a thread to an ip"""
    while True:
        #grabs ip,cmd from queue
        ip = q.get()
        print "Thread %s: Running %s to %s" % (i,cmd,ip)
        host = "root@%s"%ip
        subprocess.call(["ssh", host, cmd])
        q.task_done()
    


if __name__ == '__main__':
    #grab ips and cmds from config
    ips,cmds = readConfig()
    
    #Determine Number of threads to use,but max out at 20
    if len(ips) < 20:
        num_threads = len(ips)
    elif len(ips) >= 20:
        num_thread = 20
    
    #Start thread pool
    for i in range(num_threads):
        for cmd in cmds:
            worker = Thread(target=launcher, args=(i,queue,cmd))
            worker.setDaemon(True)
            worker.start()
    print "Main Thread Witing"
    for ip in ips:
        queue.put(ip)
    queue.join()
    end = time.time()
    print "Dispatch Completed in %s seconds" % (end - start)



