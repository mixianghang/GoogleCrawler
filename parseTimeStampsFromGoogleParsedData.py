import os, sys,re, json

#assign each filtered url with date info
'''
 urlListWithDates: in googleQuery dir
 filteredTrueFile in fitleredpages dir
 resultFile in filteredpages dir
'''
def mapTimeStampsForFilteredUrlList():
  if len(sys.argv) < 4:
    print "Usage urlListWithDates filteredTrueFile resultFile"
    sys.exit(1)
  urlListWithDate = sys.argv[1]
  filteredUrlFile = sys.argv[2]
  resultFile = sys.argv[3]
  
  urlMap = {}
  with open(urlListWithDate, "r") as fd:
    for line in fd:
      attrs = line.strip().split("\t")
      if len(attrs) != 2:
        print "error when parsing {0}".format(line)
        sys.exit(1)
      if attrs[1] == "none":
        urlMap[attrs[0]] = None
      else:
        urlMap[attrs[0]] = attrs[1]
  resultMap = {}
  withoutMapNum = 0
  with open(filteredUrlFile, "r") as fd:
    for line in fd:
      entry = json.loads(line.strip())
      if entry["result"] != "class" and entry["result"] != "brand":
        continue
      url = entry["url"]
      if url.lower() == "nourl":
        continue
      if url not in urlMap:
        print "error: filtered url not shown in overall list: {0}".format(url)
        withoutMapNum += 1
        continue
        #sys.exit(1)
      resultMap[url] = urlMap[url]
  print "got {0} filtered urls with map, {1} without".format(len(resultMap), withoutMapNum)
  noneNum = 0
  dateNum = 0
  with open(resultFile, "w") as resultFd:
    for entryUrl in resultMap:
      dateStr = resultMap[entryUrl]
      if dateStr is None:
        noneNum += 1
        resultFd.write("{0}\tnone\n".format(entryUrl))
      else:
        resultFd.write("{0}\t{1}\n".format(entryUrl, dateStr))
        dateNum += 1
  print "{0} unique,  {1} with date, {2} without".\
  format(len(resultMap),  dateNum, noneNum)
def parseTimeStampsFromGoogleParsedData():
  if len(sys.argv) < 3:
    print "Usage googleResultJson resultFile"
    sys.exit(1)
  
  sourceFile = sys.argv[1]
  resultFile = sys.argv[2]
  resultMap = {}
  entryNum = 0
  noneNum = 0
  dateNum = 0
  with open(sourceFile, "r") as sourceFd:
    for line in sourceFd: 
      query = json.loads(line.strip())
      entryList = query["entities"]
      for entry in entryList:
        url = entry["url"]
        entryNum += 1
        if url in resultMap and resultMap[url] is not None:
          continue
        if "dateStr" not in entry:
          resultMap[url] = None
          continue
        dateStr = entry["dateStr"]
        if dateStr == "none":
          resultMap[url] = None
          continue
        elif dateStr == "parseerror":
          resultMap[url] = None
          continue
        resultMap[url] = dateStr
  with open(resultFile, "w") as resultFd:
    for entryUrl in resultMap:
      dateStr = resultMap[entryUrl]
      if dateStr is None:
        noneNum += 1
        resultFd.write("{0}\tnone\n".format(entryUrl))
      else:
        resultFd.write("{0}\t{1}\n".format(entryUrl, dateStr))
        dateNum += 1
  print "got {0} duplicated entries, {1} unique,  {2} with date, {3} without".\
  format(entryNum, len(resultMap),  dateNum, noneNum)

if __name__ == "__main__":
  mapTimeStampsForFilteredUrlList()

