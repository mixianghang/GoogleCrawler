#!/usr/bin/python
import os, sys
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
sys.path.append("../")
import loggingUtil
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
  import urlparse  as urlparse
else:
  import urllib.urlparse as urlparse
reload(sys)
sys.setdefaultencoding("utf-8")

def getRequestWithoutUa(url):
  try:
    response = requests.get(url, timeout = timeout , verify = False, stream = stream)
  except Exception as e:
    logger.debug("get error %s for url %s", e, url)
    response = Response()
    response.status_code = 207
  return response

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
timeout = (5, 10)#connection timeout: 5seconds, read timeout : 5seconds
stream = False
specialCases = {
  "forbes.com": getRequestWithoutUa,
}

class ResponseStatus(object):
  SSL_ERROR = 8880
  URL_TIMEOUT = 8886
  REQUEST_EXCEPTION = 8887
  COST_TOO_LONG = 8888

class CrawlThread(Thread):
  def __init__(self, resultDir, urlList, isSession = True, **kwargs):
    self.ignoreList = [404, 400, 410,999]
    self.resultDir = resultDir
    self.urlList = urlList
    self.respList = []
    self.errRespList = []
    self.retryList = []
    self.isSession = isSession
    self.session = requests.Session()
    self.session.timeout = timeout
    self.session.verify = False
    #configure fake user agent because servers may block you or disable part of services 
    #ua = UserAgent()
    #userAgentForChrome = {"User-Agent":ua.chrome}
    latestUa = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36"
    userAgentForChrome = {"User-Agent": latestUa}
    self.session.headers.update(userAgentForChrome)
#there may be ftp
    a = requests.adapters.HTTPAdapter(max_retries=3)
    self.session.mount("http://", a)
    self.session.stream = stream
    self.threadTimeout = 60 * len(self.urlList) #2 hours
    self.urlTimeout = 300 # 5mins
    super(CrawlThread, self).__init__(**kwargs)
  def run(self):
    startTime = time.time()
    for url in self.urlList:
      if (time.time() - startTime) >= self.threadTimeout:
        self.retryList.append(url)
        responseStatus = {
          "url" : url,
          "requestUrl" : url,
          "status" : ResponseStatus.COST_TOO_LONG,#timeout 
          "msg" : "timeout",
        }
        self.errRespList.append(responseStatus)
        continue
      try:
        self.__run__(url)
      except ssl.SSLError:
        responseStatus = {
          "url" : url,
          "requestUrl" : url,
          "status" : ResponseStatus.SSL_ERROR,#timeout 
          "msg" : "unavoided ssh handshake error",
          "traceback" : traceback.format_exc(),
        }
        self.errRespList.append(responseStatus)
      #except: requests.Timeout:
      #except: requests.ConnectionError:
      except requests.RequestException:
        #logger.warning("got warning when requesting %s url with traceback %s", url, traceback.format_exc())
        responseStatus = {
          "url" : url,
          "requestUrl" : url,
          "status" : ResponseStatus.REQUEST_EXCEPTION,#unknown err
          "msg" : "request error",
          "traceback" : traceback.format_exc(),
        }
        self.errRespList.append(responseStatus)
      except:
        logger.error("got error when requesting %s url with traceback %s", url, traceback.format_exc())
        self.retryList.append(url)
        responseStatus = {
          "url" : url,
          "requestUrl" : url,
          "status" : 8887,#unknown err
          "msg" : "unknow exception"
        }
        self.errRespList.append(responseStatus)
  def __run__(self, url):
    urlParts = urlparse.urlparse(url)
    if len(urlParts.netloc) <= 0:
      logger.error("the request url cannot be parsed correctly %s", url)
      return
    if "forbes.com" in urlParts.netloc:
      response = getRequestWithoutUa(url)
    elif self.isSession:
      response = self.session.get(url, timeout = self.session.timeout)
    else:
      response = getRequest(url)
    if response.status_code == 207:
      self.errRespList.append({"url" : url, "status" : 207})
      self.retryList.append(url)
      return
    responseStatus = {"url" : response.url}
    responseStatus["requestUrl"] = url
    responseStatus["status"] = response.status_code
    if response.status_code != 200:
      if response.status_code == 503 or response.status_code == 302:
        time.sleep(10)
        response = getRequestWithoutUa(response.request.url)
        if response.status_code != 200:
          gotError = True
          logger.error("COMMONERROR Retry for 503 or 302 got error response code with status:%s", json.dumps(responseStatus))
          if response.status_code not in self.ignoreList:
            self.retryList.append(responseStatus["requestUrl"])
          self.errRespList.append(responseStatus)
          return 
      else:
        logger.error("COMMONERROR got error response code with status:%s", json.dumps(responseStatus))
        if response.status_code not in self.ignoreList:
          self.retryList.append(responseStatus["requestUrl"])
        self.errRespList.append(responseStatus)
        return

    if "content-type" in response.headers:
      responseStatus["content-type"] = response.headers["content-type"].lower()
    else:
      responseStatus["content-type"] = "missedInHeaders"
    responseStatus["requestUrlHash"] = fileHash(response.request.url)
    if "text/plain" in responseStatus["content-type"]:
      responseStatus["contentHash"] = "undownloadedForTxt"
    #elif "application" in responseStatus["content-type"]:
    else:
      responseStatus["contentHash"] = responseStatus["requestUrlHash"]
      try:
        startTime = time.time()
        with open(os.path.join(self.resultDir, responseStatus["contentHash"]), "wb") as resultFd:
          #shutil.copyfileobj(response.raw, f)
          for chunk in response.iter_content(chunk_size = 1024 * 128):
            if (time.time()  - startTime) >= self.urlTimeout:
              responseStatus["status"] = ResponseStatus.URL_TIMEOUT
              responseStatus["msg"] = "response receiving timeout"
              self.retryList.append(responseStatus["requestUrl"])
              self.errRespList.append(responseStatus)
              response.close()
              logger.error("time out when receiving response from %s", responseStatus["requestUrl"])
              return
            if chunk:
              resultFd.write(chunk)
              #resultFd.flush()
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
    #else:
    #  responseStatus["contentHash"] = responseStatus["requestUrlHash"]
    #  try:
    #    with open(os.path.join(entryPagesDir, responseStatus["contentHash"]), "wb") as resultFd:
    #      #shutil.copyfileobj(response.raw, f)
    #      for chunk in response.iter_content(chunk_size = 1024 * 512):
    #        if chunk:
    #          resultFd.write(chunk)
    #          #resultFd.flush()
    #  except:
    #    logger.error("write to file error with traceback %s for url %s", traceback.format_exc()\
    #    , responseStatus["requestUrl"])
    #    self.retryList.append(responseStatus["requestUrl"])
    #    response.close()
    #    continue
    response.close()
    self.respList.append(responseStatus)
    return

def fileHash(fileContent):
  return hashlib.sha256(fileContent).hexdigest()

class Response(object):
  status_code = 200


def getRequest(url):
  try:
    response = requests.get(url, timeout = timeout , verify = False, headers=userAgentForChrome, stream = stream)
  except Exception as e:
    logger.debug("get error %s for url %s", e, url)
    response = Response()
    response.status_code = 207
  return response

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
  if isStd:
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
  logger.info('Logger initialized success')
  return logger

def singleProcess(params, shared):
  name = mp.current_process().name
  try:
    result = singleCrawler(params, shared)
  except Exception as e:
    logger.error("process %s  exited with error %s and traceback %s", name, e, traceback.format_exc())
    shared[name] = {
      "status" : 1,
    }
    return
  
    
def singleCrawler(params, shared):
  global logger
  startTime = time.time()
  urlList = params["urlList"]
  resultDir = params["resultDir"]
  entryPageDir = params["entryPageDir"]
  logDir = params["logDir"]
  threadNum = params["threadNum"]
  isSession = params["isSession"]
  name = mp.current_process().name
  pid = os.getpid()
  if not os.path.exists(resultDir):
    os.makedirs(resultDir)
  if not os.path.exists(logDir):
    os.makedirs(logDir)
  logger = initLog(logDir)
  logger.debug("start process %s with pid %d", name, pid)
  partNum = len(urlList) / threadNum
  urlIndex = 0
  failedNum = 0
  successNum = 0
  processTimeLimit = 3600 * 24
  while urlIndex < len(urlList):
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
    for i in range(threadNum):
      subUrlList = urlList[urlIndex : urlIndex + 10]
      thread = CrawlThread(entryPageDir, subUrlList, isSession = isSession)
      threadList.append(thread)
      urlIndex += 10
      if urlIndex >= len(urlList):
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
  retryFd.write("\n".join(retryList))
  queryResultFd.close()
  queryErrorResultFd.close()
  retryFd.close()

def main():
  startTime = time.time()
  parser = argparse.ArgumentParser(description = "args for google search")
  parser.add_argument("-s", "--isSession", action = "store_true")
  parser.add_argument("-st", "--isStream", action = "store_true")
  parser.add_argument("-th", "--threadNum", type = int, default = 50, help = "set thread num of the pool")
  parser.add_argument("-p", "--procNum", type = int, default = 10, help = "set procNum num of the pool")
  parser.add_argument("urlList", type = str)
  parser.add_argument("resultDir", type = str)
  if len(sys.argv) < 3:
    parser.print_help()
    sys.exit(1)
  options = parser.parse_args()
  global stream
  if options.isStream:
    stream = True
  else:
    stream = False
  if not os.path.exists(options.resultDir):
    os.makedirs(options.resultDir)
  entryPageDir = os.path.join(options.resultDir, "entrypages")
  if not os.path.exists(entryPageDir):
    os.makedirs(entryPageDir)
  logger = initLog(options.resultDir, isStd = True)
  urlList = open(options.urlList, "r").read().splitlines()
  procNum = options.procNum
  logger.debug("got %d urls", len(urlList))
  if len(urlList) < procNum:
    procNum = len(urlList)
  partNum = len(urlList) / procNum
  jobs = []
  jobDataDict = {}
  manager = Manager()
  sharedDict = manager.dict()
  for i in range(0, procNum):
    if i == (procNum - 1):
      partList = urlList[partNum * i : ]
    else:
      partList = urlList[partNum * i : partNum * (i + 1)]
    name = "process_{0}".format(i + 1)
    resultDir = os.path.join(options.resultDir, name)
    params = {
      "urlList" : partList,
      "isSession" : options.isSession,
      "threadNum" : options.threadNum,
      "entryPageDir" : entryPageDir,
      "resultDir" : resultDir,
      "logDir" : resultDir,
    }
    jobDataDict[name] = (partList, resultDir)
    process = mp.Process(target = singleProcess, args = (params,sharedDict), name = name)
    jobs.append(process)
  for i in range(0, len(jobs)):
    jobs[i].start()

  retryList = []
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
    returnCode = jobs[i].exitcode
    name = jobs[i].name
    logger.debug("%s processs exited with code %d", jobs[i].name, returnCode)
    tempDict = dict(sharedDict)
    item = tempDict[name]
    logger.debug("got shared infor from %s: %s", name, item)
    logger.debug("current shared dict %s", sharedDict)
    partList = jobDataDict[name][0]
    retryFile = os.path.join(jobDataDict[name][1], RETRY_FILE);
    if item["status"] == 0:
      successNum += item["success"]
      failedNum += item["failed"]
      if item["failed"] > 0:
        subRetryList = open(retryFile, "r").read().splitlines()
        if len(subRetryList) != item["failed"]:
          logger.error("error for %s with unmatched retryList data %s", name, item)
        retryList.extend(subRetryList)
      subRespFile = os.path.join(jobDataDict[name][1], RESP_FILE);
      subRespErrFile = os.path.join(jobDataDict[name][1], RESP_ERR_FILE);
      with open(subRespFile, "r") as subFd:
        for line in subFd:
          respFd.write(line)
      with open(subRespErrFile, "r") as subFd:
        for line in subFd:
          respErrFd.write(line)
    else:
      failedNum += len(partList)
      retryList.extend(partList)
    shutil.rmtree(jobDataDict[name][1])

  if failedNum > 0:
    with open(os.path.join(options.resultDir, RETRY_FILE), "a") as retryFd:
      retryFd.write("\n".join(retryList))

  respFd.close()
  respErrFd.close()
  logger.debug("Overall %d, success %d, failed %d", len(urlList), successNum, failedNum)
  #TODO retry failed urls
  endTime = time.time()
  logger.debug("time cost is %d", endTime - startTime)

if __name__ == '__main__':
  main()
