import os,sys
import json
import codecs

if len(sys.argv) < 5:
  print "Usage: overallUrlList, retrievedUrlList, resultFile unexistList"
  sys.exit(1)

overallUrlList = open(sys.argv[1], "r").read().splitlines()
#type(overallUrlList[94667])
overallMap = {}
for url in overallUrlList:
  overallMap[url] = 1

usedUrlList = []
with open(sys.argv[2], "r") as fd:
  lineNum = 0
  for line in fd:
	lineNum += 1
	try:
	  attrs = json.loads(line.strip())
	except:
	  print "error when load line {0}: {1}".format(lineNum, line)
	  sys.exit(1)
	usedUrlList.append(attrs["requestUrl"])

unexistList = []
for url in usedUrlList:
  url = str(url)
  if url not in overallMap:
	unexistList.append(url)
	#print "{0} not exists in overallMap".format(url)
	continue
  overallMap[url] = 0

unusedUrlList = []
for url in overallMap:
  if overallMap[url] == 1:
	unusedUrlList.append(url)
print "got {0} overall Urls, {1} used Urls, {2} unused Url, {3} unexist".format(len(overallUrlList), len(usedUrlList)\
, len(unusedUrlList), len(unexistList))

open(sys.argv[3], "w").write("\n".join(unusedUrlList))
open(sys.argv[4], "w").write("\n".join(unexistList))

