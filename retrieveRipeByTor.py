#!/usr/bin/python
import sys
import os
import ConfigParser
from pprint import pprint
import time
from retrieveUtil import *
import threading
from subprocess import call
import requesocks
from torController import *

def renewConnFunc(mainsession, ips, blackFileFd=None):
    #print "restart tor service"
    #call("sudo service tor restart", shell = True)
    #print "sleep five seconds after restart tor"
    #time.sleep(70)
    try:
      currIp = mainsession.get("http://httpbin.org/ip").text
      decoded = json.loads(currIp)['origin']
    except Exception as e:
      sys.stderr.write(repr(e))
      print repr(e) 
    else:
      if decoded in ips:
        print "restart doesn't get a new ip"
      else:
        print "restart gets a new Ip is {0}".format(decoded)
        print ips
        if blackFileFd is not None:
          blackFileFd.write(decoded +"\n")
        ips.add(decoded)
        return 0
    retry = 0
    while True:
      time.sleep(60 + 10 * retry)
      print "start to renew connection"
      try:
        response = renewConn()
      except Exception as e:
        print "renew failed, sleep and wait"
        retry += 1
        continue
      print "finish renew connection"
      if response == -1:
        print "renew failed,sleep and wait"
        retry += 1
        continue
      try:
        currIp = mainsession.get("http://httpbin.org/ip").text
        decoded = json.loads(currIp)['origin']
      except Exception as e:
        sys.stderr.write(repr(e))
        print repr(e) 
        continue
      if decoded in ips:
        retry += 1
        print "renew failed , retry {0}, sleep to wait {1} seconds".format(retry, 60 + 10* retry)
        continue
      else:
        print "new Ip is {0}".format(decoded)
        print ips
        if blackFileFd is not None:
          blackFileFd.write(decoded +"\n")
        ips.add(decoded)
        break
    return 0

#time measurement example
#start = time.clock()
#end = time.clock()
#print "time elapsed in seconds:", (end - start)
date=time.strftime("%Y-%m-%d-%H-%M-%S")

if len(sys.argv) < 4:
  print >>sys.stderr, "Usage: py configFile blackipFile usedIpFile" 
  sys.exit(1)
#read config file and get a list of configs
configFile=sys.argv[1]
blackIpFile = sys.argv[2]
usedIpFile = sys.argv[3]
if not os.path.exists(configFile):
  sys.stderr.write("config file doesn't exist: %s" %configFile)
  sys.exit(1)
if not os.path.exists(blackIpFile):
  sys.stderr.write("blackIp file doesn't exist: %s" %blackIpFile)
  sys.exit(1)

configParser = ConfigParser.SafeConfigParser({"startPos": 0, "threadNum": 10})
configParser.read(configFile)
sections = configParser.sections()
#read ip list
with open(blackIpFile, "r") as f:
  ipList = f.read().splitlines()
ips = set(ipList)
print "ip blacklist"
print ips

blackIpFd = open(usedIpFile, "a")

if sections is None:
  print >>sys.stderr, "no config items in config file: %s" % (configFile)
  sys.exit(1)

numLimit = 4500
mainsession = requesocks.session()
# Tor uses the 9050 port as the default socks port
mainsession.proxies = {'http':  'socks5://127.0.0.1:9050',
                   'https': 'socks5://127.0.0.1:9050'}

for section in sections:
  print section
  keyList = configParser.get(section, "keylistPath")
  startPos = configParser.getint(section, "startPos")
  requestUrl = configParser.get(section, "requestUrl")
  resultFile = configParser.get(section,"resultFile")
  threadNum  = configParser.getint(section,"threadNum")
  print "%s %s %s" %(keyList, requestUrl, resultFile)
  if not os.path.exists(keyList):
    error("keylist file doesn't exist:{0}".format(keyList))
    sys.exit(1)
  if os.path.exists(resultFile):
    try:
      os.rename(resultFile, resultFile + "_bak_" +date)
    except Exception as e:
      error("rename result file failed: {0}".format(resultFile))
      sys.exit(1)
  if not os.path.exists(os.path.dirname(resultFile)):
      os.makedirs(os.path.dirname(resultFile))
  #open keylist file and loop to send http request and save response
  keyListFd = open(keyList, "r")
  kwNum = lineCount(keyList)
  kwList = keyListFd.readlines()
  keyListFd.close()
  partFiles = []
  if threadNum <= 0:
    print "thread num cannot be zero"
    sys.exit(1)
  for i in range(0, threadNum):
    partResultFile = "{0}_part{1}".format(resultFile, i + 1)
    partFiles.append(partResultFile)
  while startPos < kwNum:
    if numLimit > (kwNum - startPos):
      partNumLimit = kwNum - startPos
    else:
      partNumLimit = numLimit
    threadPart = partNumLimit / threadNum
    nextPos = startPos + partNumLimit
    partPos = startPos
    threads = []
    RetrieveThread.partCount = 0
    RetrieveThread.partErrorCount = 0
    print "start a new round with requests {0} from {1} to {2}".format(partNumLimit, startPos, nextPos)
    for i in range(0, threadNum):
      session = requesocks.session()
      # Tor uses the 9050 port as the default socks port
      session.proxies = {'http':  'socks5://127.0.0.1:9050',
                         'https': 'socks5://127.0.0.1:9050'}
      partResultFile = partFiles[i]
      if (i + 1) != threadNum:
          newThread = RetrieveThread(i + 1, kwList[partPos : partPos + threadPart], requestUrl, partResultFile,session)
      else:
          newThread = RetrieveThread(i + 1, kwList[partPos:nextPos], requestUrl, partResultFile, session)
      newThread.start()
      threads.append(newThread)
      partPos += threadPart
    startPos = nextPos
    print "start to sniff whether to renew connection"
    while True:
      if RetrieveThread.partCount >= partNumLimit:
        break
      if RetrieveThread.errorIndicate == 1:
        print "we need to renew connections"
        renewConnFunc(mainsession, ips, blackIpFd)
        RetrieveThread.errorIndicate = 0
      time.sleep(5)
    for thread in threads:
        thread.join()
    sumReqs = RetrieveThread.requestCount
    partReqs = RetrieveThread.partCount
    errorReqs = RetrieveThread.errorCount
    partError = RetrieveThread.partErrorCount
    print "request sum: {0}, this time: {1}, errorSum: {2}, thisError:{3}".format(sumReqs, partReqs, errorReqs, partError);
    if startPos >= kwNum:
      break
    print "sleep before next round"
    time.sleep(70)
    print "renew before next round"
    renewConnFunc(mainsession, ips, blackIpFd)
    if blackIpFd is not None:
      blackIpFd.flush()
  if blackIpFd is not None:
    blackIpFd.close()
  command = "cat {0} > {1}".format(" ".join(partFiles), resultFile)
  call(command, shell=True)
  for partFile in partFiles:
      os.remove(partFile)
#end = time.clock()
#print "time elapsed in seconds:", (end - start)
#end = time.clock()
#print "time elapsed in seconds:", (end - start)
#loop through configs, for each config, for each config, read key list, for each key, retrieve referenced object and save to file
