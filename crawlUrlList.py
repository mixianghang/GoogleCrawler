#!/usr/bin/python
import requests
import os, sys
from bs4 import BeautifulSoup
import hashlib
import json
import argparse
import torController
import logging
import time
from multiprocessing.dummy import Pool as ThreadPool 
from multiprocessing import Pool
import loggingUtil
from fake_useragent import UserAgent

ua = UserAgent()
latestUa = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36"
userAgentForChrome = {"User-Agent":latestUa}
class Response(object):
  status_code = 200
def getRequest(url):
  try:
    response = requests.get(url, verify = False, headers=userAgentForChrome)
  except Exception as e:
    logger.debug("get error %s", e)
    response = Response()
    response.status_code = 207
  return response
    
  
if sys.version_info < (3, 0):
  import urlparse  as urlparse
else:
  import urllib.urlparse as urlparse
reload(sys)
sys.setdefaultencoding("utf-8")

ONE_DAY = 24 * 3600
STATUS_OK = 200
SEP = "\t"
def fileHash(fileContent):
  return hashlib.sha256(fileContent).hexdigest()

def queryUrls():
  startTime = time.time()

  parser = argparse.ArgumentParser(description = "args for one leve url queries")
  parser.add_argument("-t","--isTor", action = "store_true",  help = "use tor to retrieve")
  parser.add_argument("-s", "--sleep", type = int, default = 10, help = "sleep time in minutes")
  parser.add_argument("-th", "--threadNum", type = int, default = 15, help = "set thread num of the pool")
  parser.add_argument("urlList", type = str)
  parser.add_argument("resultDir", type = str)
  if len(sys.argv) < 3:
    parser.print_help()
    sys.exit(1)

  options = parser.parse_args()
  if options.isTor:
    session = torController.getSession()
  else:
    session = requests.Session()
  session.timeout = (10, 30)
  ua = UserAgent()
  userAgentForChrome = {"User-Agent":ua.chrome}
  session.headers.update(userAgentForChrome)
  resultDir = options.resultDir
  urlFile = options.urlList
  sleepTime = options.sleep * 60
  pool = ThreadPool(options.threadNum)
  threadNum = options.threadNum
  if not os.path.exists(resultDir):
    os.makedirs(resultDir)
#THIS IS used to store crawled pages
  entityPagesDir = os.path.join(resultDir, "entitypages")
  if not os.path.exists(entityPagesDir):
    os.makedirs(entityPagesDir)

  urlList = []
  with open(urlFile, "r") as urlFd:
    for line in urlFd:
      attrs = line.strip().split("\t")
      urlList.append(attrs[1])
  print "got {0} urls".format(len(urlList))
  #this is used to store response status
  responseStatusFd = open(os.path.join(resultDir, "entityPageResponseList.json"), "w")
  urlNo = 0
  timeOut = (5, 10)
  while urlNo < len(urlList):
    #subUrl = urlList[urlNo]
    #urlNo += 1
    #rawResponse = requests.get(subUrl, timeout = timeOut)
    #rawResponseList = [rawResponse]
    #logger.debug("current url no is %d", urlNo)
    #logger.debug("start url request %s", subUrl)
    #logger.debug("end url request %s", subUrl)
    subUrlList = urlList[urlNo:urlNo + threadNum]
    rawResponseList = pool.map(getRequest, subUrlList)
    urlNo += threadNum
    logger.debug("current url no is {0}".format(urlNo))
    gotError = False
    responseErrorList = []
    for response in rawResponseList:
      if response.status_code == 207:
        continue
      responseStatus = {"finalUrl" : response.url}
      responseStatus["requestUrl"] =  response.request.url
      if "content-type" in response.headers:
        responseStatus["content-type"] = response.headers["content-type"]
      else:
        responseStatus["content-type"] = "unknown"
      responseStatus["status"] = response.status_code
      responseStatus["requestUrlHash"] = fileHash(response.request.url)
      if "application" in responseStatus["content-type"]:
        responseStatus["contentHash"] = "undownloaded"
      else:
        hash = fileHash(response.text)
        responseStatus["contentHash"] = hash
        open(os.path.join(entityPagesDir, responseStatus["contentHash"]), "w").write(response.text.encode("utf-8"))
      responseStatusFd.write(json.dumps(responseStatus) + "\n")
      if response.status_code != 200:
        logger.debug("got error response code with status:%s", json.dumps(responseStatus))
        gotError = True
        responseErrorList.append(responseStatus)
    if gotError:
      logger.error("get error response %s", json.dumps(responseErrorList))
  logger.debug("time cost is %d seconds", time.time() - startTime)
  responseStatusFd.close()

if __name__ == "__main__":
  logger = loggingUtil.initLog()
  queryUrls()
