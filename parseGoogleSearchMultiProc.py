#!/usr/bin/python
import requests
import os, sys
from bs4 import BeautifulSoup
import hashlib
import json
import argparse
import logging
import time
from multiprocessing.dummy import Pool as ThreadPool 
from multiprocessing import Pool
from multiprocessing import Process
import multiprocessing
import loggingUtil
import traceback
import codecs
import re
import urllib.parse  as urlparse
import shutil

STATUS_OK = 200
SEP = "\t"
chosenParser = "lxml"

def multiproc():
  global chosenParser
  startTime = time.time()
  logger = loggingUtil.initLog()
  parser = argparse.ArgumentParser(description = "args for google search")
  parser.add_argument("googleQueriesDir", type = str)
  parser.add_argument("resultDir", type = str)
  parser.add_argument("-p", "--processNum", type = int, default = 20)
  if len(sys.argv) < 3:
    parser.print_help()
    sys.exit(1)
  options = parser.parse_args()
  chosenParser = "lxml"
#make dirs if not existed
  if not os.path.exists(options.resultDir):
    os.makedirs(options.resultDir)
  resultJsonFd = open(os.path.join(options.resultDir, "googlePageParseResult.json"), "w")
  googlepageDir = os.path.join(options.googleQueriesDir, "entrypages")
  queryResultFile = os.path.join(options.googleQueriesDir, "UrlQueryResultList")
  queryList = open(queryResultFile, "r").read().splitlines()
  logger.debug("got %d queries from file %s", len(queryList), queryResultFile)
  partNum = len(queryList) // options.processNum
  subResultDirList = []
  jobs = []
  for i in range(options.processNum):
    name = "process_{0}".format(i + 1)
    subResultDir = os.path.join(options.resultDir, name)
    subResultDirList.append(subResultDir)
    if not os.path.exists(subResultDir):
      os.makedirs(subResultDir)
    if i == options.processNum - 1:
      subQueryList = queryList[i * partNum :]
    else:
      subQueryList = queryList[i * partNum : (i + 1) * partNum]
    process = Process(target = parseGoogle, args = (subResultDir, googlepageDir, subQueryList), name = name)
    jobs.append(process)
  for process in jobs:
    process.start()
  for process in jobs:
    process.join()
    returnCode = process.exitcode
    if returnCode != 0:
      logger.error("process %s return error", process.name)
      #sys.exit(1)
  for subResultDir in subResultDirList:
    jsonFile = os.path.join(subResultDir, "googlePageAbstract.json")
    resultJsonFd.write(open(jsonFile, "r").read())
    shutil.rmtree(subResultDir)
  logger.debug("time cost is %d", time.time() - startTime)

def parseGoogle(resultDir, googlepageDir, queryList):
  try:
    dateRe = re.compile("([a-z]{3} \d+, \d{4})", re.I | re.U)
    resultStatRe = re.compile("([\d,]+) *results", re.I | re.U)
    stripNumRe = re.compile("[ ,]+", re.I)
    startTime = time.time()
    logger = loggingUtil.initLog()
    logger.debug(" googlepage Dir %s, resultDir %s, parser %s and %d quries", \
    googlepageDir, resultDir, chosenParser, len(queryList))
    if not os.path.exists(resultDir):
      os.makedirs(resultDir)
    pageDir = googlepageDir
    resultJsonFd = open(os.path.join(resultDir, "googlePageAbstract.json"), "w")
    pageNum = 0
    entryNum = 0
    for line in queryList:#loop query result by line
      query = json.loads(line.strip())
      pageNum += 1
      if len(query) < 4:#there should be 4 attributes
        logger.error("parse query result line failed : %s", line)
        sys.exit(1)
      requestUrl = query["url"] 
      status = int(query["status"])# status can be converted to int
      hash = query["contentHash"]
      responseStatus = query
      pageFile = os.path.join(pageDir, hash)
      try:
        pageStr = codecs.open(pageFile, mode = "r", encoding = "utf-8").read()
        soup = BeautifulSoup(pageStr, chosenParser)
        resultStat = soup.select("div#resultStats")
        if len(resultStat) < 1:
          logger.error("no result stats for file %s", hash)
          resultNum = -1
        else:
          text = resultStat[0].getText().strip()
          resultNumStr = resultStatRe.search(text).group(1)
          resultNum = int(stripNumRe.sub("", resultNumStr))
        #divSgList = soup.select("div._NId div.srg div.g")#TODO currently ignore other type of entries
        #divGList = soup.select("div._NId  div.g")#TODO currently ignore other type of entries
        #print("num of divSg {}, divG {}".format(len(divSgList), len(divGList)))
        #divList = divSgList + divGList
        #logger.debug("got %d hrefs for request %s", len(divList), requestUrl)
        divList = soup.select("div._NId div.g")#TODO currently ignore other type of entries
        if len(divList) <= 0:
          logger.debug("got 0 entries for request %s", line)
        entityList = []
        index = 0
        for div in divList:
          index += 1
          aTagList = div.select('h3.r > a')
          if len(aTagList) <= 0:
            logger.warning("warning when parsing div %d of %s: without <a> tag", index, hash)
            continue
          aTag = aTagList[0]
          href = aTag["href"]
          if len(href) <= 0:
            logger.warning("parse entry url error for {}\n".format(aTag.prettify()))
            continue
          if type(href) is list:
            href = href[0]
          title = aTag.get_text()
          spanTagList = div.select('span.st')
          if len(spanTagList) <= 0:
            logger.warning("parse error for file %s and div %d: no excerpt", hash, index)
            entity = {
              "title" : title,
              "url" : href,
              "excerpt" : "",
            }
          else:
            spanTag = spanTagList[0]
            dateSpan = spanTag.select("span.f")
            if dateSpan is None or len(dateSpan) == 0:
              dateStr = "none"
            else:
              matchObj = dateRe.match(dateSpan[0].get_text())
              if matchObj is None:
                #logger.warning("parse date string error for file %s: %s", hash, dateSpan[0].get_text())
                dateStr = None
              else:
                dateStr = matchObj.group(1)
            entity = {
              "title" : title,
              "url" : href,
              "excerpt" : spanTag.get_text(),
              "dateStr" : dateStr
            }
          entityList.append(entity)

        h3LinkList = [] #retrieve title and linkes of each entry header
        h3List = soup.select("div._NId h3.r")#TODO currently ignore other type of entries
        index = 0
        for h3 in h3List:
          index += 1
          aTagList = h3.select('a')
          if len(aTagList) <= 0:
            logger.warning("error when parsing h3 %d of %s: without <a> tag", index, hash)
            continue
          aTag = aTagList[0]
          href = aTag["href"]
          if len(href) <= 0:
            logger.warning("parse entry url error for {}\n".format(aTag.prettify()))
            continue
          if type(href) is list:
            href = href[0]
          title = aTag.get_text()
          h3LinkList.append({"title" : title, "url" : href})
        entryNum += len(entityList)

        hasRCol = False
        linksOfRCol = []
        rCol = soup.select("div#rhs div#rhs_block div.g")
        if len(rCol) > 0:
          hasRCol = True
          rLinkTags = rCol[0].select("a")
        else:
          hasRCol = False
          rLinkTags = []
        mapUrlNum = 0
        for rLink in rLinkTags:
          href = rLink["href"]
          if "/maps/" in href:
            mapUrlNum += 1
          title = rLink.get_text().strip()
          linksOfRCol.append({"title" : title, "url" : href,})
        jsonResult = {
          "responseStatus" : responseStatus,
          "resultNum" : resultNum,
          "entities" : entityList,
          "titleLinkList" : h3LinkList,
          "hasRCol" : hasRCol,
          "linksOfRCol" : linksOfRCol,
          "mapLinkNumInRCol" : mapUrlNum,
        }
        resultJsonFd.write(json.dumps(jsonResult) + "\n")
      except:
        logger.error("error when parsing page %s with traceback %s", hash, traceback.format_exc())
    resultJsonFd.close()
    logger.debug("time cost is %d seconds", time.time() - startTime)
  except Exception as e:
    logger.error("process %s got exception %s with traceback %s for result Dir %s", \
    multiprocessing.current_process().name, e, traceback.format_exc(), resultDir)

if __name__ == "__main__":
  multiproc()

