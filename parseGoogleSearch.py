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
from fake_useragent import UserAgent
import loggingUtil
if sys.version_info < (3, 0):
  import urlparse  as urlparse
else:
  import urllib.urlparse as urlparse
reload(sys)
sys.setdefaultencoding("utf-8")

STATUS_OK = 200
SEP = "\t"

def parseGoogle():
  startTime = time.time()
  logger = loggingUtil.initLog()
  parser = argparse.ArgumentParser(description = "args for google search")
  parser.add_argument("sourceDirList", type = str)
  parser.add_argument("resultDir", type = str)
  if len(sys.argv) < 3:
	parser.print_help()
	sys.exit(1)
  options = parser.parse_args()
  sourceDirList = open(options.sourceDirList, "r").read().splitlines()
  logger.debug("got %d source dirs", len(sourceDirList))
#make dirs if not existed
  if not os.path.exists(options.resultDir):
	os.makedirs(options.resultDir)
  resultJsonFd = open(os.path.join(options.resultDir, "googlePageAbstract.json"), "w")
  resultUrlFd = open(os.path.join(options.resultDir, "entryUrlList"), "w")
  entityUrlSet = set()
  pageNum = 0
  entryNum = 0
  for sourceDir in sourceDirList:
	for processBasename in os.listdir(sourceDir):
	  processDir = os.path.join(sourceDir, processBasename)
	  if not os.path.isdir(processDir):
		continue
	  logger.debug("current parse dir %s", processDir)
	  queryResultFile = os.path.join(processDir, "googleQueryResultList")
	  pageDir = os.path.join(processDir, "googlepages")
	  with open(queryResultFile, "r") as queryResultFd:
	    for line in queryResultFd:#loop query result by line
		  pageNum += 1
		  attrs = line.strip().split("\t")
		  if len(attrs) < 4:#there should be 4 attributes
			logger.error("parse query result line failed : %s", line)
			sys.exit(1)
		  requestUrl = attrs[1]
		  status = int(attrs[2])# status can be converted to int
		  hash = attrs[3]
		  responseStatus = {
			"url" : attrs[0],
			"requestUrl" : attrs[1],
			"status" : attrs[2],
			"hash" : attrs[3],
		  }
		  pageFile = os.path.join(pageDir, hash)
		  pageStr = open(pageFile, "r").read()
		  soup = BeautifulSoup(pageStr, "html.parser")
		  divList = soup.select("div.g")
		  logger.debug("got %d hrefs for request %s", len(divList), requestUrl)
		  if len(divList) <= 0:
			logger.warning("got 0 entries for request %s", line)
		  entityList = []
		  for div in divList:
			aTag = div.select('h3.r > a')[0]
			href = aTag["href"]
			#combs = urlparse.urlparse(href)
			#paramsDict = urlparse.parse_qs(combs.query)
			#if "q" not in paramsDict:
			#  logger.warning("unrecognized url %s", href)
			#  href = ""
			#  continue
			#else:
			#  href = paramsDict["q"][0]
			if len(href) <= 0:
			  logger.warning("parse entry url error for {0}\n".format(aTag.prettify()))
			if type(href) is list:
			  href = href[0]
			title = aTag.get_text()
			entityUrlSet.add(href)
			spanTagList = div.select('span.st')
			if len(spanTagList) <= 0:
			  logger.warning("parse error for file %s and div %s", pageFile, div)
			  entity = {
				"title" : title,
				"url" : href,
				"excerpt" : "",
			  }
			else:
			  spanTag = spanTagList[0]
			  entity = {
				"title" : title,
				"url" : href,
				"excerpt" : spanTag.get_text(),
			  }
			entityList.append(entity)
		  entryNum += len(entityList)
		  jsonResult = {
			"responseStatus" : responseStatus,
			"entities" : entityList,
		  }
		  resultJsonFd.write(json.dumps(jsonResult) + "\n")
  resultUrlFd.write("\n".join(list(entityUrlSet)))
  logger.debug("got %d unique entries, %d entries(including duplicates) from %d query pages",\
   len(entityUrlSet), entryNum, pageNum)
  resultJsonFd.close()
  resultUrlFd.close()
  logger.debug("time cost is %d seconds", time.time() - startTime)

if __name__ == "__main__":
  parseGoogle()

