#!/usr/bin/python
import json
import os,sys
from random import shuffle
import re
import urllib.request, urllib.parse, urllib.error
from datetime import datetime
from datetime import timedelta
import loggingUtil
import argparse

logger = loggingUtil.initLog()
def main():
  defaultQueryString = "home automation hack attack vulnerability flaws threat smarthome"
  #defaultFiletypes = " -filetype:pdf AND -filetype:ppt AND -filetype:doc"
  defaultFiletypes = ""
  parser = argparse.ArgumentParser(description = "compose time-based queries")
  parser.add_argument("-q", "--query", type = str, default = defaultQueryString)
  parser.add_argument("-qf", "--queryFile", type = str)
  parser.add_argument("-pn", "--pageNum", type = int, default = 1)
  parser.add_argument("-num", "--numEachPage", type = int, default = 100)
  parser.add_argument("-ft", "--filetypes", type = str, default = defaultFiletypes)
  parser.add_argument("-s", "--startDate", help = "month/day/Year", type = str, default = "01/01/2015")
  parser.add_argument("resultFile", type = str)
  if len(sys.argv) < 2:
    parser.print_help()
    sys.exit(1)
  options = parser.parse_args()

  baseUrl =  "https://google.com/search/ncr?num={}&lr=lang_en".format(options.numEachPage)
  queryTimeFormat = "cdr:1,cd_min:{0},cd_max:{1}"
  dateFormat = "%m/%d/%Y"
  today = datetime.today()
  startDate = datetime.strptime(options.startDate, dateFormat)
  filetypes = options.filetypes

  urlList = []
  queryList = []
  if options.queryFile:
    queryList  = open(options.queryFile, "r").read().splitlines()
  else:
    queryList.append(options.query)


  for query in queryList:
    #compose all time queries
    for i in range(0, options.pageNum):
      params = {
        "q" : query + " " + filetypes,
        "start" : i * 100,
      }
      qStr = urllib.parse.urlencode(params)
      qUrl = baseUrl + "&" + qStr
      urlList.append(qUrl)

  if False:
    for query in queryList:
      #compose queries for every week
      weekDelta = timedelta(weeks = 1)
      currDate = datetime.strptime(options.startDate, dateFormat)
      while currDate <= today:
        startDate = currDate.strftime(dateFormat)
        endDate = (currDate + weekDelta).strftime(dateFormat)
        logger.debug("%s, %s", startDate, endDate)
        queryTimeLimit = queryTimeFormat.format(startDate, endDate)
        params = {
          "q" : query + " " + filetypes,
          "tbs" :queryTimeLimit,
        }
        qStr = urllib.parse.urlencode(params)
        qUrl = baseUrl + "&" + qStr
        urlList.append(qUrl)
        currDate = currDate + weekDelta
#comose monthly queries
      monthDelta = timedelta(days = 30)
      currDate = datetime.strptime(options.startDate, dateFormat)
      while currDate <= today:
        startDate = currDate.strftime(dateFormat)
        endDate = (currDate + monthDelta).strftime(dateFormat)
        logger.debug("%s, %s", startDate, endDate)
        queryTimeLimit = queryTimeFormat.format(startDate, endDate)
        params = {
          "q" : query + " " + filetypes,
          "tbs" :queryTimeLimit,
        }
        qStr = urllib.parse.urlencode(params)
        qUrl = baseUrl + "&" + qStr
        urlList.append(qUrl)
        currDate = currDate + monthDelta

  logger.debug("got {0} urls".format(len(urlList)))
#write back to file
  open(options.resultFile, "w").write("\n".join(urlList))
if __name__ == "__main__":
  main()
