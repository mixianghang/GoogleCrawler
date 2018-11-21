#!/usr/bin/python
import os, sys,re 
import codecs
#from bs4 import BeautifulSoup
import hashlib
import json
import argparse
import shutil
import logging
import ssl
import time
import requests
from multiprocessing.dummy import Pool as ThreadPool 
from multiprocessing import Pool, Queue,Manager
from  multiprocessing import Process
import multiprocessing as mp
import traceback
import shutil
#import torController
import logUtil
import imp
sys.path.append("../")
import __main__ as main
from sys import platform
if platform == "darwin":
  from requests.packages.urllib3.exceptions import InsecureRequestWarning
  from requests.packages.urllib3.exceptions import InsecurePlatformWarning
  requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
  requests.packages.urllib3.disable_warnings(InsecurePlatformWarning)
from requests.packages.urllib3.exceptions import InsecureRequestWarning
#from requests.packages.urllib3.exceptions import InsecurePlatformWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
#requests.packages.urllib3.disable_warnings(InsecurePlatformWarning)
import threading
from threading import Thread
import urllib.parse as urlparse

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, sdch, br',
    'Accept-Language': 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4',
}
logger = None
sharedDict = None
RETRY_FILE = "retryUrlList"
RESP_FILE = "SugQueryResultList"
RESP_ERR_FILE = "SugQueryErrorResultList"
ONE_DAY = 24 * 3600
STATUS_OK = 200
SEP = "\t"
timeout = (10, 20)#connection timeout: 5seconds, read timeout : 5seconds
stream = False
BASE_URL = "https://ifttt.com/applets/"
sleepTime = 30#sleep 10 seconds for thread
loginRe = re.compile("\/login", re.I)
class ResponseStatus(object):
  SSL_ERROR = 8880
  CONNECT_EXCEPTION = 8884
  TIMEOUT_EXCEPTION = 8885
  URL_TIMEOUT = 8886
  REQUEST_EXCEPTION = 8887
  COST_TOO_LONG = 8888
  LOGIN_PAGE = 8889

respErrorRe = re.compile("HTTP/[\d.]+ (\d{3}) ", re.I | re.U)
stripRe = re.compile("[\t\n\r]+", re.I | re.U)
kwDoneDict = {}
kwRetryDict = {}
kwErrorList = []
hitCount = 0
def getSug(kw, depth = 3, session = None, proxies = None, triggerList = [], tryLimit = 3):
  if depth == 0:
    return []
  baseUrl = "http://suggestqueries.google.com/complete/search"
  queryParams = {
    "output" : "chrome",
    "hl" : "en",
    "q" : kw.encode("utf-8"),
  }
  queryStr = urlparse.urlencode(queryParams)
  requestUrl = baseUrl + "?" + queryStr
  if session is None:
    session = requests.Session()
  resultList = []
  newTriggerList = triggerList + [kw]
  tryTime = 0
  global hitCount
  while True:
    tryTime += 1
    try:
      if kw in kwDoneDict:
        subKwList = kwDoneDict[kw]
        hitCount += 1
      else:
        response = session.get(requestUrl, headers = headers, proxies = proxies)
        if response.status_code != 200:
          if response.status_code == 403:
            tmpSleepTime = sleepTime * tryTime
            if tmpSleepTime <= 900:
              logger.debug("got 403 status, sleep {} seconds".format(tmpSleepTime))
              time.sleep(tmpSleepTime)
              tryTime += 1
              continue
          if response.status_code == 400:#url as request
            logger.warning("stop because of 400 response status for request %s: %s", requestUrl, response.status_code)
            return resultList
          if tryTime < tryLimit:
            logger.warning("retry because of invalid response status for request %s: %s with msg %s", requestUrl, response.status_code, response.text)
            continue
          else:
            logger.error("failed because of invalid response status for request \t%s\t: %s with msg %s", requestUrl, response.status_code, response.text)
            errorMsg = {
              "status" : response.status_code,
              "triggerList" : newTriggerList,
            }
            kwRetryDict[kw] = 1
            kwErrorList.append(errorMsg)
            return resultList
        jsonObj = json.loads(response.text)
        respKw = jsonObj[0]
        tempSubKwList = jsonObj[1]
        subKwList = []
        for subKw in tempSubKwList:
          subKw = stripRe.sub(" ", subKw)
          subKwList.append(subKw)
      for subKw in subKwList:
        if subKw == kw:
          continue
        resultList.append(newTriggerList + [subKw])
      for subKw in subKwList:
        if subKw in newTriggerList:
          continue
        subResultList = getSug(subKw, depth = depth - 1, session = session, triggerList = newTriggerList)
        resultList.extend(subResultList)
      if kw not in kwDoneDict:
        kwDoneDict[kw] = subKwList
      kwRetryDict.pop(kw, None)
      return resultList
    except Exception as e:
      if tryTime < tryLimit:
        logger.warning("retry because of exception for request %s: %s with trace %s", requestUrl, e, traceback.format_exc())
        continue
      logger.error("failed because of exception for request %s: %s with trace %s", requestUrl, e, traceback.format_exc())
      errorMsg = {
        "status" : 207,
        "msg" : "exception",
        "triggerList" : newTriggerList,
      }
      kwRetryDict[kw] = 1
      kwErrorList.append(errorMsg)
      return  resultList

class CrawlThread(Thread):
  def __init__(self, resultDir, urlList, depth = 3,  proxyThread = None, **kwargs):
    self.ignoreList = []
    self.resultDir = resultDir
    self.depth = depth
    self.proxyThread = proxyThread
    if self.proxyThread:
      self.isProxy = True
    else:
      self.isProxy = False
    self.proxy = None
    self.retryTime = 0
    self.urlList = urlList
    self.respList = []
    self.errRespList = []
    self.retryList = []
    self.isSession = True
    self.session = requests.Session()
    self.session.timeout = timeout
    self.session.verify = False
    self.session.headers.update(headers)
    self.session.stream = stream
    self.threadTimeout = 60 * len(self.urlList) #2 hours
    self.urlTimeout = 300 # 5mins
    super(CrawlThread, self).__init__(**kwargs)
  def run(self):
    if self.isProxy:
      self.proxy = getProxy(self.proxyThread)
      logger.debug("thread %s, new proxy is %s", self.getName(), self.proxy)
    startTime = time.time()
    urlNum = len(self.urlList)
    currIndex = 0
    retryTime = 0
    while currIndex < urlNum:
      kw = self.urlList[currIndex]
      resultList = getSug(kw, depth = self.depth, session = self.session, proxies = self.proxy)
      self.respList.extend(resultList)
      currIndex += 1

def fileHash(fileContent):
  return hashlib.md5(fileContent.encode("utf-8")).hexdigest()

def singleProcess(params, shared):
  name = mp.current_process().name
  try:
    result = singleCrawler(params, shared)
  except Exception as e:
    print((traceback.format_exc()))
    logger.error("process %s  exited with error %s and traceback %s", name, e, traceback.format_exc())
    shared[name] = {
      "status" : 1,
    }
    return
  
    
def getProxy(proxyThread):
  if proxyThread is None:
    return None
  #proxyLock.acquire()
  proxy = proxyThread.getRandomProxy(subTypeList = ["HTTPS", "SOCKS5", "SOCKS4"], latencyLimit = 5000)
  #time.sleep(1)
  #proxyLock.release()
  if proxy is None:
    return None
  ip = proxy["ip"]
  port = proxy["port"]
  typeList = proxy["typeList"]
  if "HTTPS" in typeList:
    proxyStr = "http://{0}:{1}".format(ip, port)
  elif "SOCKS5" in typeList:
    proxyStr = "socks5://{0}:{1}".format(ip, port)
  else:
    proxyStr = "socks4://{0}:{1}".format(ip, port)
  proxySetting = { 
      "http" : proxyStr,
      "https" : proxyStr,
  }
  return proxySetting

proxyThread = None
proxyMutex = None
def singleCrawler(params, shared):
  name = mp.current_process().name
  global logger
  depth = params["depth"]
  logDir = params["logDir"]
  resultDir = params["resultDir"]
  isProxy = params["isProxy"]
  if isProxy:
    proxyDir = params["proxyDir"]
    proxyInterval = params["proxyInterval"]
    global proxyThread # create proxy thread
    global proxyLock
    from crawlProxyList import ProxyThread
    from threading import Lock
    proxyThread = ProxyThread(proxyDir, updateInterval = proxyInterval)
    proxyLock = Lock()
    proxyThread.start()
    while True:
      if proxyThread.isProxyReady():
        break
      else:
        time.sleep(5)
    if not os.path.exists(resultDir):
      os.makedirs(resultDir)
  if not os.path.exists(logDir):
    os.makedirs(logDir)
  #logger = logUtil.initLog(logDir, isStd = True, name = name)
  urlList = params["urlList"]
  startId = 0
  endId = len(urlList)
  entryPageDir = params["entryPageDir"]
  threadNum = params["threadNum"]
  startTime = time.time()
  pid = os.getpid()
  logger.debug("start process %s with pid %d", name, pid)
  urlIndex = 0
  failedNum = 0
  successNum = 0
  processTimeLimit = 10 * (endId - startId)
  indexId = startId
  threshold = 1000
  lowerBound = 50
  numOfRecipes = endId - startId + 1
  if numOfRecipes <= threshold * threadNum:
    threshold = numOfRecipes // threadNum
  if threshold == 0:
    threshold = 10
  global kwDoneDict
  global kwRetryDict
  global kwErrorList
  while indexId < endId:
    retryList = []
    responseList  = []
    errRespList = []
    kwDoneDict = {}
    kwRetryDict = {}
    kwErrorList = []
    if (time.time() - startTime) >= processTimeLimit:
      failedNum += len(retryList)
      retryList.extend(urlList[urlIndex:])
      saveData(resultDir, retryList, responseList, errRespList)
      logger.error("time out for process with retry number %d", len(retryList))
      break
    threadList = []
    logger.debug("thread number is %d and threshold is %d", threadNum, threshold)
    for i in range(threadNum):
      subStartId = indexId
      subEndId = indexId + threshold
      if subEndId >= endId:
        subEndId = endId
      thread = CrawlThread(entryPageDir, urlList[subStartId : subEndId], proxyThread = proxyThread, depth = depth)
      threadList.append(thread)
      indexId = subEndId
      if indexId >= endId:
        break
    for thread in threadList:
      thread.start()
    for thread in threadList:
      thread.join()
    for thread in threadList:
      retryList.extend(thread.retryList)
      responseList.extend(thread.respList)
      errRespList.extend(thread.errRespList)
    failedNum += len(kwRetryDict)
    successNum += len(responseList)
    saveData(resultDir, kwRetryDict.keys(), responseList, kwErrorList)
  logger.debug("time cost is %d seconds", time.time() - startTime)
  if isProxy:
    proxyThread.stopProxy()
  #pass back to main process
  name = mp.current_process().name
  pid = os.getpid()
  shared[name] = {
    "status" : 0,
    "failed" : failedNum,
    "success" : successNum,
  }#process name, pid, statusCode, failedNUm, successNum
  logger.debug("failed {0} requests and success {1}, hit count {2} with return status 0".format(failedNum, successNum, hitCount))
  return

def saveData(resultDir, retryList, responseList, errRespList):
  #write request statistics to file
  queryResultFd = open(os.path.join(resultDir, RESP_FILE), "a")
  queryErrorResultFd = open(os.path.join(resultDir, RESP_ERR_FILE), "a")
  retryFd = open(os.path.join(resultDir, RETRY_FILE), "a")
  for response in responseList:
    queryResultFd.write("\t->\t".join(response) + "\n")
  for errResp in errRespList:
    queryErrorResultFd.write(json.dumps(errResp) + "\n")
  if len(retryList) > 0:
    retryFd.write("\n".join(retryList) + "\n")
  queryResultFd.close()
  queryErrorResultFd.close()
  retryFd.close()

def main():
  startTime = time.time()
  parser = argparse.ArgumentParser(description = "args for google search")
  parser.add_argument("-th", "--threadNum", type = int, default = 10, help = "set thread num of the pool")
  parser.add_argument("-p", "--procNum", type = int, default = 10, help = "set procNum num of the pool")
  parser.add_argument("-ip","--isProxy", action = "store_true")
  parser.add_argument("-pi","--proxyInterval", type = int, default = 300)
  parser.add_argument("-ld", "--logdir", default = "./")
  parser.add_argument("-depth", "--depth", default = 3, type = int)
  parser.add_argument("urlFile", type = str)
  parser.add_argument("resultDir", type = str)
  if len(sys.argv) < 3:
    parser.print_help()
    sys.exit(1)
  options = parser.parse_args()
  if not os.path.exists(options.resultDir):
    os.makedirs(options.resultDir)
  entryPageDir = os.path.join(options.resultDir, "entrypages")
  if not os.path.exists(entryPageDir):
    os.makedirs(entryPageDir)
  global logger
  logger = logUtil.initLog(options.logdir, functionName = "crawlGoogleSuggestionResult", isStd = True)
  procNum = options.procNum
  urlList = codecs.open(options.urlFile, mode = "r", encoding = "utf-8").read().splitlines()
  numOfQueries = len(urlList)
  logger.debug("got %d google query urls", numOfQueries)
  if numOfQueries < procNum:
    procNum = numOfQueries
  partNum = numOfQueries // procNum
  jobs = []
  jobDataDict = {}
  manager = Manager()
  sharedDict = manager.dict()
  currIndex = 0
  for i in range(0, procNum):
    if i == (procNum - 1):
      subUrlList = urlList[currIndex:]
    else:
      subUrlList = urlList[currIndex: currIndex + partNum]
    currIndex += partNum
    name = "process_{0}".format(i + 1)
    resultDir = os.path.join(options.resultDir, name)
    proxyDir = None
    if options.isProxy:
      proxyDir = os.path.join(options.resultDir, "proxies")
      if not os.path.exists(proxyDir):
        os.makedirs(proxyDir)
    params = {
      "urlList" : subUrlList,
      "threadNum" : options.threadNum,
      "entryPageDir" : entryPageDir,
      "resultDir" : resultDir,
      "logDir" : resultDir,
      "proxyDir" : proxyDir,
      "proxyInterval" : options.proxyInterval,
      "isProxy" : options.isProxy,
      "depth" : options.depth,
    }
    jobDataDict[name] = (subUrlList, resultDir)
    process = mp.Process(target = singleProcess, args = (params,sharedDict), name = name)
    jobs.append(process)
  for i in range(0, len(jobs)):
    jobs[i].start()

  successNum = 0
  failedNum = 0
  responseFile = os.path.join(options.resultDir, RESP_FILE)
  respErrFile = os.path.join(options.resultDir, RESP_ERR_FILE)
  respFd = open(responseFile, "w")
  respErrFd = open(respErrFile, "w")
  finishedMap = {}
  i = 0
  while True:
    target = None
    i = 0
    while True:
      if jobs[i].is_alive():
        i += 1
        if i == procNum:
          time.sleep(5)
          i = 0
        continue
      if len(finishedMap) == procNum:
        break
      if i in finishedMap:
        i += 1 
        if i == procNum:
          time.sleep(5)
          i = 0
        continue
      else:
        finishedMap[i] = 1
        target = i
        break
    if target is None and len(finishedMap) == procNum:
      break
    jobs[i].join()
    retryList = []
    returnCode = jobs[i].exitcode
    name = jobs[i].name
    logger.debug("%s processs exited with code %d", jobs[i].name, returnCode)
    tempDict = dict(sharedDict)
    item = tempDict[name]
    logger.debug("got shared infor from %s: %s", name, item)
    logger.debug("current shared dict %s", sharedDict)
    subUrlList = jobDataDict[name][0]
    subDir = jobDataDict[name][1]
    retryFile = os.path.join(subDir, RETRY_FILE);
    if item["status"] == 0:
      successNum += item["success"]
      failedNum += item["failed"]
      if item["failed"] > 0:
        subRetryList = open(retryFile, "r").read().splitlines()
        if len(subRetryList) != item["failed"]:
          logger.error("error for %s with unmatched retryList data %s", name, item)
        retryList.extend(subRetryList)
      subRespFile = os.path.join(subDir, RESP_FILE);
      subRespErrFile = os.path.join(subDir, RESP_ERR_FILE);
      with open(subRespFile, "r") as subFd:
        for line in subFd:
          respFd.write(line)
      with open(subRespErrFile, "r") as subFd:
        for line in subFd:
          respErrFd.write(line)
    else:
      failedNum += len(subUrlList)
      for url in subUrlList:
        retryList.append(url)
    shutil.rmtree(subDir)
    if len(retryList) > 0:
      with open(os.path.join(options.resultDir, RETRY_FILE), "a") as retryFd:
        retryFd.write("\n".join(retryList) + "\n")


  respFd.close()
  respErrFd.close()
  logger.debug("tried %d queries, get success %d, failed %d", \
  len(urlList), successNum, failedNum)
  #TODO retry failed urls
  endTime = time.time()
  logger.debug("time cost is %d", endTime - startTime)

if __name__ == '__main__':
  main()
