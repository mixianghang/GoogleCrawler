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
if sys.version_info < (3, 0):
  import urllib.parse  as urlparse
else:
  import urllib.parse as urlparse

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, sdch, br',
    'Accept-Language': 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4',
}
latestUa = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36"
userAgentForChrome = {"User-Agent":latestUa}
logger = None
sharedDict = None
RETRY_FILE = "retryUrlList"
RESP_FILE = "UrlQueryResultList"
RESP_ERR_FILE = "UrlQueryErrorResultList"
ONE_DAY = 24 * 3600
STATUS_OK = 200
SEP = "\t"
timeout = (10, 20)#connection timeout: 5seconds, read timeout : 5seconds
stream = False
BASE_URL = "https://ifttt.com/applets/"
sleepTime = 10#sleep 10 seconds for thread
loginRe = re.compile("\/login", re.I)
proxyUrl = "http://gw.proxies.online:8081"
staticProxy = {
  "http" : proxyUrl, 
  "https" : proxyUrl,
}
class ResponseStatus(object):
  SSL_ERROR = 8880
  CONNECT_EXCEPTION = 8884
  TIMEOUT_EXCEPTION = 8885
  URL_TIMEOUT = 8886
  REQUEST_EXCEPTION = 8887
  COST_TOO_LONG = 8888
  LOGIN_PAGE = 8889

respErrorRe = re.compile("HTTP/[\d.]+ (\d{3}) ", re.I | re.U)

class CrawlThread(Thread):
  def __init__(self, resultDir, urlList, proxy = None, **kwargs):
    self.ignoreList = [404]
    self.resultDir = resultDir
    self.proxy = proxy
    if self.proxy:
      self.isProxy = True
    else:
      self.isProxy = False
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
    #if self.isProxy:
    #  self.proxy = getProxy(self.proxyThread)
    #  logger.debug("thread %s, new proxy is %s", self.getName(), self.proxy)
    startTime = time.time()
    urlNum = len(self.urlList)
    currIndex = 0
    retryTime = 0
    while currIndex < urlNum:
      url = self.urlList[currIndex]
      url = url.replace("ncr/", "")
      try:
        result = self.__run__(url)
        if result == -1:
          self.session = requests.Session()
          self.session.timeout = timeout
          self.session.verify = False
          self.session.headers.update(headers)
          self.session.stream = stream
          continue
      except ssl.SSLError:
        responseStatus = {
          "url" : url,
          "requestUrl" : url,
          "status" : ResponseStatus.SSL_ERROR,#timeout 
          "msg" : "unavoided ssh handshake error",
          "traceback" : traceback.format_exc(),
        }
        self.errRespList.append(responseStatus)
        self.retryList.append(url)
      except requests.Timeout:#retry at most 3 times before logging error and move forward
        self.changeSession()
        if retryTime < self.retryTime:
          retryTime += 1
          continue
        else:
          #self.proxy = getProxy(self.proxyThread)
          #logger.debug("thread %s chage new  proxy for connection error is %s", self.getName(), self.proxy)
          retryTime = 0
          continue
        responseStatus = {
          "url" : url,
          "requestUrl" : url,
          "status" : ResponseStatus.TIMEOUT_EXCEPTION,#unknown err
          "msg" : "timeout error",
          "traceback" : traceback.format_exc(),
        }
        self.errRespList.append(responseStatus)
        self.retryList.append(url)
      except requests.ConnectionError:
        self.changeSession()
        if retryTime < self.retryTime:
          retryTime += 1
          continue
        else:
          #self.proxy = getProxy(self.proxyThread)
          #logger.debug("thread %s chage new  proxy for connection error is %s", self.getName(), self.proxy)
          retryTime = 0
          continue
        responseStatus = {
          "url" : url,
          "requestUrl" : url,
          "status" : ResponseStatus.CONNECT_EXCEPTION,#unknown err
          "msg" : "timeout error",
          "traceback" : traceback.format_exc(),
        }
        self.errRespList.append(responseStatus)
        self.retryList.append(url)
      except requests.RequestException:
        #logger.warning("got warning when requesting %s url with traceback %s", url, traceback.format_exc())
        responseStatus = {
          "url" : url,
          "requestUrl" : url,
          "status" : ResponseStatus.REQUEST_EXCEPTION,#unknown err
          "msg" : "request error",
          "traceback" : traceback.format_exc(),
        }
        self.retryList.append(url)
        self.errRespList.append(responseStatus)
      except:
        logger.error("got error when requesting %s url with traceback %s", url, traceback.format_exc())
        self.retryList.append(url)
        responseStatus = {
          "url" : url,
          "requestUrl" : url,
          "status" : 8887,#unknown err
          "msg" : "exception with traceback {0}".format(traceback.format_exc())
        }
        self.errRespList.append(responseStatus)
        self.retryList.append(url)
      currIndex += 1
      retryTime = 0
  def changeSession(self):
    self.session = requests.Session()
    self.session.timeout = timeout
    self.session.verify = False
    self.session.headers.update(headers)
    self.session.stream = stream
  def __run__(self, url):
    startTime = time.time()
    response = self.session.get(url, timeout = self.session.timeout, proxies = self.proxy)
    if respErrorRe.match(response.text):
      logger.debug("response error with msg %s, change proxy", response.text)
      return -1
    if (response.status_code == 503 or response.status_code == 403) and self.isProxy:#blocked
      logger.debug("get blocked, change proxy Ip")
      return -1
    logger.debug("thread {4} got response for {3} with code {2}, length {0} and cost {1} seconds".format(len(response.text), time.time() - startTime, response.status_code, url, self.getName()))
    if time.time() - startTime >= 30:
      self.changeSession()
    if response.status_code == 207:
      self.errRespList.append({"url" : url, "status" : 207})
      self.retryList.append(url)
      return
    responseStatus = {"url" : response.url}
    responseStatus["requestUrl"] = url
    responseStatus["status"] = response.status_code
    #print responseStatus
    if response.status_code != 200:
      if response.status_code not in self.ignoreList:
        logger.error("COMMONERROR got error response code with status:%s", json.dumps(responseStatus))
        self.retryList.append(responseStatus["requestUrl"])
      self.errRespList.append(responseStatus)
      return
    else:
      pass
      #logger.debug("got 200 response with length {0} and cost {1} seconds".format(len(response.text), time.time() - startTime))

    if "content-type" in response.headers:
      responseStatus["content-type"] = response.headers["content-type"].lower()
    else:
      responseStatus["content-type"] = "missedInHeaders"
    responseStatus["requestUrlHash"] = fileHash(url)
    if "text/plain" in responseStatus["content-type"]:
      responseStatus["contentHash"] = "undownloadedForTxt"
    #elif "application" in responseStatus["content-type"]:
    else:
      responseStatus["contentHash"] = responseStatus["requestUrlHash"]
      try:
        startTime = time.time()
        fd = codecs.open(os.path.join(self.resultDir, responseStatus["contentHash"]), mode = "w"\
        , encoding = "utf-8")
        fd.write(response.text)
        fd.close()
        timeCost = time.time() - startTime
        responseStatus["timeCost"] = timeCost
      except:
        logger.error("write to file error with traceback %s for url %s", traceback.format_exc()\
        , responseStatus["requestUrl"])
        self.retryList.append(responseStatus["requestUrl"])
        responseStatus["msg"] = "write to file failed"
        responseStatus["status"] = 208
        self.errRespList.append(responseStatus)
        response.close()
        return
    response.close()
    self.respList.append(responseStatus)
    return

def fileHash(fileContent):
  return hashlib.md5(fileContent.encode("utf-8")).hexdigest()

def initLog(logDir, isStd = False):
  mainFile = os.path.basename(__file__)
  logfile = time.strftime('%Y-%m-%d-%H', time.localtime()) + "-" + mainFile + '.log'
  logfile = os.path.join(logDir, logfile)
  logger = logging.getLogger('logger')
  formatter = logging.Formatter('%(process)d-%(processName)s %(asctime)s - %(levelname)s - \
  [%(filename)s: %(lineno)s - %(funcName)s] %(message)s')
  logger.setLevel(logging.DEBUG)
  fh = logging.FileHandler(logfile)
  fh.setLevel(logging.DEBUG)
  fh.setFormatter(formatter)
  logger.addHandler(fh)
  #if isStd:
  #  ch = logging.StreamHandler(sys.stdout)
  #  ch.setLevel(logging.DEBUG)
  #  ch.setFormatter(formatter)
  #  logger.addHandler(ch)
  logger.info('Logger initialized success, haha')
  return logger

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
  logDir = params["logDir"]
  resultDir = params["resultDir"]
  isProxy = params["isProxy"]
  if isProxy:
    proxy = staticProxy
  else:
    proxy = None
  if not os.path.exists(resultDir):
    os.makedirs(resultDir)
  if not os.path.exists(logDir):
    os.makedirs(logDir)
  logger = logUtil.initLog(logDir, isStd = True, name = name)
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
  while indexId < endId:
    retryList = []
    responseList  = []
    errRespList = []
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
      thread = CrawlThread(entryPageDir, urlList[subStartId : subEndId], proxy = proxy)
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
    failedNum += len(retryList)
    successNum += len(responseList)
    saveData(resultDir, retryList, responseList, errRespList)
  logger.debug("time cost is %d seconds", time.time() - startTime)
  #pass back to main process
  name = mp.current_process().name
  pid = os.getpid()
  shared[name] = {
    "status" : 0,
    "failed" : failedNum,
    "success" : successNum,
  }#process name, pid, statusCode, failedNUm, successNum
  logger.debug("failed {0} requests and success {1} with return status 0".format(failedNum, successNum))
  return

def saveData(resultDir, retryList, responseList, errRespList):
  #write request statistics to file
  queryResultFd = open(os.path.join(resultDir, RESP_FILE), "a")
  queryErrorResultFd = open(os.path.join(resultDir, RESP_ERR_FILE), "a")
  retryFd = open(os.path.join(resultDir, RETRY_FILE), "a")
  for response in responseList:
    queryResultFd.write(json.dumps(response) + "\n")
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
  parser.add_argument("-ld", "--logdir", default = "./")
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
  logger = logUtil.initLog(options.logdir, functionName = "crawlGoogleQueryResult", isStd = True)
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
    params = {
      "urlList" : subUrlList,
      "threadNum" : options.threadNum,
      "entryPageDir" : entryPageDir,
      "resultDir" : resultDir,
      "logDir" : resultDir,
      "isProxy" : options.isProxy,
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
  respFd = open(responseFile, "a")
  respErrFd = open(respErrFile, "a")
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
