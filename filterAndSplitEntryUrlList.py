#!/usr/bin/python
import os,sys
import argparse
import re
def filter(urlList):
  ftpRe = re.compile("^ftp", re.I)
  ftpNum = 0
  acmRe = re.compile(".*acm\.org/", re.I)
  acmNum = 0
  linkedinRe = re.compile(".*linkedin\.com", re.I)
  linkedinNum = 0
  httpRe = re.compile("^https?", re.I)
  unformattedNum = 0
  resultUrlList = []
  for url in urlList:
	if ftpRe.match(url):
	  ftpNum += 1
	  continue
	if acmRe.match(url):
	  acmNum += 1
	  continue
	if linkedinRe.match(url):
	  linkedinNum += 1
	  continue
	if httpRe.match(url) is None:
	  print url
	  unformattedNum += 1
	  continue
	resultUrlList.append(url)
  print "exclude {0} ftp urls, {1} acm urls, {2} linkedin urls, unformatted {3}"\
  .format(ftpNum, acmNum, linkedinNum, unformattedNum)
  return resultUrlList
if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("urlFile")
  parser.add_argument("resultDir")
  parser.add_argument("-s", "--isSplit",  action = "store_true")
  parser.add_argument("-f", "--isFilter",  action = "store_true")
  parser.add_argument("-sn", "--splitNum", default = 10, type = int)

  if len(sys.argv) < 2:
    parser.print_help()
    sys.exit(1)
  options = parser.parse_args()
  if not os.path.exists(options.resultDir):
	os.makedirs(options.resultDir)
  urlList = open(options.urlFile, "r").read().splitlines()
  print "got {0} urls".format(len(urlList))
  if options.isFilter:
	urlList = filter(urlList)
	print "got {0} filtered urls".format(len(urlList))
  if options.isSplit:
	urlPartNum = len(urlList) / options.splitNum
	for i in range(options.splitNum):
	  resultPartFile = os.path.join(options.resultDir, "urlPart_{0}".format(i))
	  if i == options.splitNum - 1:
		partUrlList = urlList[ urlPartNum * i : ]
	  else:
		partUrlList = urlList[ urlPartNum * i : (i + 1) * urlPartNum]
	  open(resultPartFile, "w").write("\n".join(partUrlList))
  
  open(os.path.join(options.resultDir, "urlList"), "w").write("\n".join(urlList))


