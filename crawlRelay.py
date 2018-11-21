#crawl using multiple servers
import pexpect
import subprocess
import sys, os, shutil
import logging
import argparse
import time

COMMAND_PROMPT = '[#$] ' ### This is way too simple for industrial use -- we will change is ASAP.
TERMINAL_PROMPT = '(?i)terminal type\?'
TERMINAL_TYPE = 'vt100'
SSH_NEWKEY = '(?i)are you sure you want to continue connecting'

retryFileName = "retryUrlList"
queryFileName = "queryUrlList.txt"
queryResultDirName = "googleResult"

def convDomain2Ip(sourceFile, resultFile, hostFile):
  import socket
  resultFd = open(resultFile, "w")
  domainSet = set()
  ipSet = set()
  lineNum = 0
  with open(sourceFile, "r") as fd:
    for line in fd:
      lineNum += 1
      if lineNum == 1:
        continue
      line = line.strip()
      attrs = line.split("\t")
      domain = attrs[0]
      room = attrs[1]
      ip = socket.gethostbyname(domain)
      domainSet.add(domain)
      ipSet.add(ip)
      resultFd.write("{}\t{}\t{}\n".format(domain, ip, room))
  print("{} domains, {} ips".format(len(domainSet), len(ipSet)))
  open(hostFile, "w").write("\n".join(list(domainSet)) + "\n")
  resultFd.close()

timeout = 36000
def loginRemoteServer(host, user, passwd):
  child = pexpect.spawnu("ssh -l {} {}".format(user, host), timeout = timeout)
  child.logfile = sys.stdout
  i = child.expect([pexpect.TIMEOUT, SSH_NEWKEY, COMMAND_PROMPT, '(?i)password', pexpect.EOF])
  if i == 0: # Timeout
    print('ERROR! could not login with SSH. Here is what SSH said:')
    print(child.before, child.after)
    print(str(child))
    return None
  if i == 1: # In this case SSH does not have the public key cached.
    child.sendline ('yes')
    child.expect ('(?i)password')
    i = 3
  elif i == 2:
    # This may happen if a public key was setup to automatically login.
    # But beware, the COMMAND_PROMPT at this point is very trivial and
    # could be fooled by some output in the MOTD or login message.
    pass
  elif i == 4:
    print("error when eof is met")
    return None

  if i == 3:
    child.sendline(passwd)
    # Now we are either at the command prompt or
    # the login process is asking for our terminal type.
    i = child.expect ([COMMAND_PROMPT, TERMINAL_PROMPT])
    if i == 1:
        child.sendline (TERMINAL_TYPE)
        child.expect (COMMAND_PROMPT)
  #
  # Set command prompt to something more unique.
  #
  #COMMAND_PROMPT = "\[Xianghang\]\$ "
  #child.sendline ("PS1='[Xianghang]\$ '") # In case of sh-style
  #i = child.expect ([pexpect.TIMEOUT, COMMAND_PROMPT], timeout=10)
  #if i == 0:
  #  print("# Couldn't set sh-style prompt -- trying csh-style.")
  #  child.sendline ("set prompt='[PEXPECT]\$ '")
  #  i = child.expect ([pexpect.TIMEOUT, COMMAND_PROMPT], timeout=10)
  #  if i == 0:
  #    print("Failed to set command prompt using sh or csh style.")
  #    print("Response was:")
  #    print(child.before)
  #    return None
  return child

def sendDir2RemoteServer(localDir, remoteHost, remoteUser, remotePasswd, remoteDir):
  child = pexpect.spawnu("scp -r {} {}@{}:".format(localDir, remoteUser, remoteHost, remoteDir), timeout = 360)
  child.logfile = sys.stdout
  i = child.expect([pexpect.TIMEOUT, SSH_NEWKEY, COMMAND_PROMPT, '(?i)password', pexpect.EOF])
  if i == 0: # Timeout
    print('ERROR! timeout. Here is what SSH said:')
    print(child.before, child.after)
    print(str(child))
    return False
  if i == 1: # In this case SSH does not have the public key cached.
    child.sendline ('yes')
    child.expect ('(?i)password')
    i = 3
  elif i == 2:
    # This may happen if a public key was setup to automatically login.
    # But beware, the COMMAND_PROMPT at this point is very trivial and
    # could be fooled by some output in the MOTD or login message.
    pass
  elif i == 4:
    print("error when eof is met")
    print(child.before, child.after)
    print(str(child))
    return False

  if i == 3:
    print(child.before, child.after)
    child.sendline(remotePasswd)
    # Now we are either at the command prompt or
    # the login process is asking for our terminal type.
    i = child.expect([COMMAND_PROMPT, TERMINAL_PROMPT, pexpect.EOF])
    if i == 2:
      #print(child.before, child.after)
      #print(str(child))
      return True
  child.close()
  return True

def downloadFileFromRemoteServer(localDir, remoteHost, remoteUser, remotePasswd, remoteDir):
  child = pexpect.spawnu("scp -r {}@{}:{} {}/".format(remoteUser, remoteHost, remoteDir, localDir), timeout = 3600)
  child.logfile = sys.stdout
  i = child.expect([pexpect.TIMEOUT, SSH_NEWKEY, COMMAND_PROMPT, '(?i)password', pexpect.EOF])
  if i == 0: # Timeout
    print('ERROR! timeout. Here is what SSH said:')
    print(child.before, child.after)
    print(str(child))
    return False
  if i == 1: # In this case SSH does not have the public key cached.
    child.sendline ('yes')
    child.expect ('(?i)password')
    i = 3
  elif i == 2:
    # This may happen if a public key was setup to automatically login.
    # But beware, the COMMAND_PROMPT at this point is very trivial and
    # could be fooled by some output in the MOTD or login message.
    pass
  elif i == 4:
    print("error when eof is met")
    print(child.before, child.after)
    print(str(child))
    return False

  if i == 3:
    print(child.before, child.after)
    child.sendline(remotePasswd)
    # Now we are either at the command prompt or
    # the login process is asking for our terminal type.
    i = child.expect([COMMAND_PROMPT, TERMINAL_PROMPT, pexpect.EOF])
    if i == 2:
      #print(child.before, child.after)
      #print(str(child))
      return True
  child.close()
  return True

remoteServer = "seclab.soic.indiana.edu"
remoteUser = "xmi"
def crawlRemoteGoogleSearch(exeDir, localDir, remoteS = remoteServer, remoteU = remoteUser, remoteP = ""):
  logger = logging.getLogger(__name__)
  baseExeName = os.path.basename(os.path.normpath(exeDir))
  remoteDir = baseExeName
  global queryFileName
  global retryFileName
  global queryResultDirName
  sendDir2RemoteServer(localDir = exeDir, remoteHost = remoteS, remoteUser = remoteU, remotePasswd = remoteP,remoteDir = remoteDir)
  remoteShell = loginRemoteServer(remoteS, remoteU, remoteP)
  #remoteShell.sendline("screen -R xianghang")
  #remoteShell.expect(COMMAND_PROMPT)
  remoteShell.sendline("cd {}".format(remoteDir))
  remoteShell.expect(COMMAND_PROMPT)
  logger.debug(remoteShell.before)

  remoteShell.sendline("rm -f ./temp > /dev/null")
  remoteShell.expect(COMMAND_PROMPT)
  #logger.debug(remoteShell.before)

  remoteShell.sendline("python3 ./crawlGoogle.py -p 20 -th 20 ./{} ./{} >> ./temp".format(queryFileName, queryResultDirName))
  remoteShell.expect(COMMAND_PROMPT)
  #logger.debug(remoteShell.before)

  remoteShell.sendline("rm -f ./result.7z >> ./temp")
  remoteShell.expect(COMMAND_PROMPT)
  #logger.debug(remoteShell.before)

  remoteShell.sendline("mv ./{0}/{1} ./{1} >> ./temp".format(queryResultDirName, retryFileName))
  remoteShell.expect(COMMAND_PROMPT)
  #logger.debug(remoteShell.before)

  remoteShell.sendline("7z a result.7z ./{0} ./{1} >> ./temp".format(queryResultDirName, retryFileName))
  if remoteShell.expect(COMMAND_PROMPT) != 0:
    logger.error("error when compressing files remotely")
  #logger.debug(remoteShell.before, remoteShell.after)

  remoteShell.sendline("rm -rf ./{} >>./temp".format(queryResultDirName))
  #remoteShell.sendline("[ -e ./{} ] && echo true || echo false".format(queryResultDirName))
  if remoteShell.expect(COMMAND_PROMPT) != 0:
    logger.error("error when deleting remotely")
  #logger.debug(remoteShell.before, remoteShell.after)

  remoteShell.sendline("rm -f ./{} >>./temp".format(queryFileName))
  remoteShell.expect(COMMAND_PROMPT)
  #logger.debug(remoteShell.before)
  remoteShell.close()

  downloadFileFromRemoteServer(localDir = localDir, remoteHost = remoteS, remoteUser = remoteU, remotePasswd = remoteP,remoteDir = remoteDir + "/result.7z")
  downloadFileFromRemoteServer(localDir = localDir, remoteHost = remoteS, remoteUser = remoteU, remotePasswd = remoteP,remoteDir = remoteDir + "/{}".format(retryFileName))
  return True

def crawlRemoteGoogleSugg(exeDir, localDir, remoteS = remoteServer, remoteU = remoteUser, remoteP = ""):
  logger = logging.getLogger(__name__)
  baseExeName = os.path.basename(os.path.normpath(exeDir))
  remoteDir = baseExeName
  global queryFileName
  global retryFileName
  global queryResultDirName
  sendDir2RemoteServer(localDir = exeDir, remoteHost = remoteS, remoteUser = remoteU, remotePasswd = remoteP,remoteDir = remoteDir)
  remoteShell = loginRemoteServer(remoteS, remoteU, remoteP)
  #remoteShell.sendline("screen -R xianghang")
  #remoteShell.expect(COMMAND_PROMPT)
  remoteShell.sendline("cd {}".format(remoteDir))
  remoteShell.expect(COMMAND_PROMPT)
  logger.debug(remoteShell.before)

  remoteShell.sendline("rm -f ./temp > /dev/null")
  remoteShell.expect(COMMAND_PROMPT)
  #logger.debug(remoteShell.before)

  remoteShell.sendline("python3 ./crawlSuggestion.py -p 20 -th 20 ./{} ./{} >> ./temp".format(queryFileName, queryResultDirName))
  remoteShell.expect(COMMAND_PROMPT)
  #logger.debug(remoteShell.before)

  remoteShell.sendline("rm -f ./result.7z >> ./temp")
  remoteShell.expect(COMMAND_PROMPT)
  #logger.debug(remoteShell.before)

  remoteShell.sendline("mv ./{0}/{1} ./{1} >> ./temp".format(queryResultDirName, retryFileName))
  remoteShell.expect(COMMAND_PROMPT)
  #logger.debug(remoteShell.before)

  remoteShell.sendline("7z a result.7z ./{0} ./{1} >> ./temp".format(queryResultDirName, retryFileName))
  if remoteShell.expect(COMMAND_PROMPT) != 0:
    logger.error("error when compressing files remotely")
  #logger.debug(remoteShell.before, remoteShell.after)

  remoteShell.sendline("rm -rf ./{} >>./temp".format(queryResultDirName))
  #remoteShell.sendline("[ -e ./{} ] && echo true || echo false".format(queryResultDirName))
  if remoteShell.expect(COMMAND_PROMPT) != 0:
    logger.error("error when deleting remotely")
  #logger.debug(remoteShell.before, remoteShell.after)

  remoteShell.sendline("rm -f ./{} >>./temp".format(queryFileName))
  remoteShell.expect(COMMAND_PROMPT)
  #logger.debug(remoteShell.before)
  remoteShell.close()

  downloadFileFromRemoteServer(localDir = localDir, remoteHost = remoteS, remoteUser = remoteU, remotePasswd = remoteP,remoteDir = remoteDir + "/result.7z")
  downloadFileFromRemoteServer(localDir = localDir, remoteHost = remoteS, remoteUser = remoteU, remotePasswd = remoteP,remoteDir = remoteDir + "/{}".format(retryFileName))
  return True

#load remote server list from file
def loadHostList(hostFile, defaultUser, defaultPass):
  logger = logging.getLogger(__name__)
  hostList = []
  with open(hostFile, "r") as fd:
    for line in fd:
      line = line.strip()
      if line.startswith("#"):
        continue
      attrs = line.split("\t")
      if len(attrs) < 1:
        logger.error("error when parsing line %s", line)
        return None
      hostAddr = attrs[0]
      if len(attrs) > 1:
        user = attrs[1]
        passwd = attrs[2]
      else:
        user = defaultUser
        passwd = defaultPass
      hostList.append((hostAddr, user, passwd))
  logger.info("finish loading %d hosts", len(hostList))
  return hostList

#count flie line number
def countFileLine(file):
  lineNum = 0
  with open(file, "r") as fd:
    for line in fd:
      lineNum += 1
  return lineNum

#crawl google queries using multiple remote servers in a loop
def crawlGoogleSearch(argv):
  logger = logging.getLogger(__name__)
  parser = argparse.ArgumentParser()
  parser.add_argument("sourceDir", type = str)
  parser.add_argument("resultDir", type = str)
  parser.add_argument("hostList", type = str)
  parser.add_argument("-st", "--sleepTimeInHour", type = int, default = 1)
  parser.add_argument("-du", "--defaultUser", type = str, default = "xmi")
  parser.add_argument("-dp", "--defaultPass", type = str, default = "xmi")
  parser.add_argument("-hi", "--hostIndex", type = int, default = 0)
  options = parser.parse_args(argv)
  exeDir = options.sourceDir
  baseResultDir = options.resultDir
  hostListFile = options.hostList
  defaultUser = options.defaultUser
  defaultPass = options.defaultPass
  hourUnit = 3600
  sleepTimeInSecond = options.sleepTimeInHour * hourUnit
  if not os.path.exists(baseResultDir):
    os.makedirs(baseResultDir)

  statFd = open(os.path.join(baseResultDir, "crawlStat.txt"), "a")
  hostList = loadHostList(hostListFile, defaultUser, defaultPass)
  if hostList is None:
    logger.error("failed to load host list")
    sys.exit(1)

  jobIndex = 0
  while True:
    resultDirName = "googleResult_{}".format(jobIndex)
    resultDir = os.path.join(baseResultDir, resultDirName)
    if not os.path.exists(resultDir):
      break
    jobIndex += 1

  jobStatList = []
  global queryFileName
  global retryFileName
  global queryResultDirName
  #iterate through the hostList to crawl the google query data
  #terminate when all hosts are tried or the query task is finished
  hostIndex = options.hostIndex
  hostNum = len(hostList)
  isBlocked = False
  blockedNum = 0
  blockDetectedTime = None
  while True:
    if hostIndex == hostNum:
      hostIndex = 0
    hostConfig = hostList[hostIndex]
    resultDirName = "googleResult_{}".format(jobIndex)
    resultDir = os.path.join(baseResultDir, resultDirName)
    initQueryFile = os.path.join(exeDir, queryFileName)
    queryCount = countFileLine(initQueryFile)
    if not os.path.exists(resultDir):
      os.makedirs(resultDir)
    hostUrl, user, passwd = hostConfig
    logger.info("current job index: %d, hostUrl %s", jobIndex, hostUrl) 
    currStartTime = time.time()
    try:
      crawlRemoteGoogleSearch(exeDir, resultDir,  remoteS = hostUrl, remoteU = user, remoteP = passwd)
      #with open(os.path.join(resultDir, retryFileName), "w") as fd:
      #  fd.write("{}\n".format(jobIndex))
    except Exception as e:
      logger.error("failed to query on host: %s with error %s", hostConfig, e)
      hostIndex += 1
      continue

    #uncompress result
    #uncompressCommand = ["7z", "x", "-o{}".format(resultDir), "{}/result.7z".format(resultDir)]
    #shellResult = subprocess.run(["7z", "x", "-o{}".format(resultDir), "{}/result.7z".format(resultDir)])
    #if shellResult.returncode != 0:
    #  logger.error("error when running shell command %s", uncompressCommand)
    #  sys.exit(1)

    currEndTime = time.time()
    logger.info("time cost of current round is %d seconds", currEndTime - currStartTime)
    retryFile = os.path.join(resultDir, retryFileName)
    retryCount = countFileLine(retryFile)
    finishedCount = queryCount - retryCount
    if finishedCount <= 0:
      shutil.rmtree(resultDir)
      break
      blockedNum += 1
      if blockedNum < hostNum:
        hostIndex += 1
        continue
      logger.info("blocked, sleep %d seconds before retry", sleepTimeInSecond)
      if isBlocked == False:
        isBlocked = True
        blockDetectedTime = time.time()
      time.sleep(sleepTimeInSecond)
      logger.info("subject to blocking for %.2f hours", (time.time() - blockDetectedTime) / 3600)
      continue
    else:
      blockedNum = 0
      if isBlocked == True:
        blockCost = time.time() - blockDetectedTime
        logger.info("blocking is over, time cost is %d", blockCost)
        isBlocked = False
    currJobStat = "JobIndex {}, server {}, {} queries, {} finished, {} to try next".format(jobIndex, hostUrl, queryCount, finishedCount, retryCount)
    statFd.write(currJobStat + "\n")
    logger.info(currJobStat)

    #shutil.copy(initQueryFile, "{}_{}".format(initQueryFile, jobIndex))
    shutil.copy(retryFile, initQueryFile)

    jobStatList.append(currJobStat)
    if retryCount == 0:
      break
    jobIndex += 1
    hostIndex += 1
  statFd.close()
  open(os.path.join(baseResultDir, "nextHostIndex.txt"), "w").write("{}".format(hostIndex + 1))
funcDict = {
    "search" : crawlGoogleSearch
    }

if __name__ == "__main__":
  logging.basicConfig(level = logging.DEBUG)
  logger = logging.getLogger(__name__)
  if sys.argv[1] not in funcDict:
    print("please choose from the following function list: {}".format(funcDict.keys))
    sys.exit(1)
  func = funcDict[sys.argv[1]]
  func(sys.argv[2:])





