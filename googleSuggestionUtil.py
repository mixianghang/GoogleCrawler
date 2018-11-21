'''
@desc functions to measure and format raw data of suggestions crawling
@author Xianghang Mi xmi@iu.edu
'''
import os, sys, re, argparse, json, codecs
from collections import defaultdict

def updateFrequency(argv):
  '''
  update frequency
  '''
  parser = argparse.ArgumentParser()
  parser.add_argument("-sf1", "--sourceFileList_old", nargs = "+", type = str)
  parser.add_argument("-sf2", "--sourceFileList_new", nargs = "+", type = str)
  parser.add_argument("-rd", "--resultDir", type = str, default = None)
  options = parser.parse_args(argv)
  kwDict = defaultdict(set)
  oldSourceFileList = options.sourceFileList_old
  newSourceFileList = options.sourceFileList_new
  resultDir = options.resultDir
  for sourceFile in newSourceFileList:
    sourceFd = codecs.open(sourceFile, mode = "r", encoding = "utf-8")
    for line in sourceFd:
      kwAcList = line.strip().split("\t->\t")
      if len(kwAcList) < 2:
        print("parse error for file and  line ", sourceFile, sourceLineNum, line.strip("\n"))
        continue
      depth = len(kwAcList) - 1
      kw = kwAcList[-2]
      ac = kwAcList[-1]
      kwDict[kw].add(ac)
  uniqueKwNum = len(kwDict)
  uniqueKwAcNum = 0
  for kw in kwDict:
    acSet = kwDict[kw]
    uniqueKwAcNum += len(acSet)
  print("unique kw: {}, kw-ac: {}, average:{:.2f}".format(uniqueKwNum, uniqueKwAcNum, float(uniqueKwAcNum) / uniqueKwNum))
  kwRemovedSet = set()
  oldKwSet = set()
  for sourceFile in oldSourceFileList:
    sourceFd = codecs.open(sourceFile, mode = "r", encoding = "utf-8")
    for line in sourceFd:
      kwAcList = line.strip().split("\t->\t")
      if len(kwAcList) < 2:
        print("parse error for file and  line ", sourceFile, sourceLineNum, line.strip("\n"))
        continue
      depth = len(kwAcList) - 1
      kw = kwAcList[-2]
      ac = kwAcList[-1]
      oldKwSet.add(kw)
      if kw not in kwDict:
        kwRemovedSet.add(kw)
        #print("no kw found in new data", kw)
        continue
      acSet = kwDict[kw]
      if ac in acSet:
        acSet.remove(ac)
  newKwSet = set(kwDict.keys())
  kwAddedSet = newKwSet - oldKwSet
  updatedKwAcNum = 0
  addedKwAcNum = 0
  kwUpdatedSet = set()
  for kw in kwDict:
    acSet = kwDict[kw]
    if len(acSet) == 0:
      continue
    if kw not in kwAddedSet:
      kwUpdatedSet.add(kw)
      updatedKwAcNum += len(acSet)
    else:
      addedKwAcNum += len(acSet)
  kwRemovedNum = len(kwRemovedSet)
  kwAddedNum = len(kwAddedSet)
  kwUpdatedNum = len(kwUpdatedSet)
  print("kw removed: {}, kw added: {}, kw updated: {}, old kw: {}, new kw: {}".format(kwRemovedNum, kwAddedNum, kwUpdatedNum, len(oldKwSet), len(newKwSet)))
  print("kw-ac: {}, acs for added kws: {}, new acs for existing kws: {}".format(uniqueKwAcNum, addedKwAcNum, updatedKwAcNum))
  if resultDir is not None:
    if not os.path.exists(resultDir):
      os.makedirs(resultDir)
    removedKwFile = os.path.join(resultDir, "removedKwList.txt")
    with codecs.open(removedKwFile, mode = "w", encoding = "utf-8") as resultFd:
      resultFd.write("\n".join(list(kwRemovedSet)) + "\n")
    addedKwFile = os.path.join(resultDir, "addededKwList.txt")
    with codecs.open(addedKwFile, mode = "w", encoding = "utf-8") as resultFd:
      resultFd.write("\n".join(list(kwAddedSet)) + "\n")
    updatedKwFile = os.path.join(resultDir, "updatedKwList.txt")
    with codecs.open(updatedKwFile, mode = "w", encoding = "utf-8") as resultFd:
      resultFd.write("\n".join(list(kwUpdatedSet)) + "\n")

    resultFile = os.path.join(resultDir, "updatedKwAcList.txt")
    with codecs.open(resultFile, mode = "w", encoding = "utf-8") as resultFd:
      for kw in kwDict:
        acSet = kwDict[kw]
        if len(acSet) == 0:
          continue
        if kw in kwUpdatedSet:
          type = "updated"
        else:
          type = "added"
        acStr = "\t".join(list(acSet))
        kwAcStr = kw + "\t{}".format(type) +  "\t->\t" + acStr + "\n"
        resultFd.write(kwAcStr)

def uniqKwAcPair(argv):
  parser = argparse.ArgumentParser()
  parser.add_argument("-sf", "--sourceFileList", nargs = "+", type = str)
  parser.add_argument("-rf", "--resultFile", type = str, default = None)
  options = parser.parse_args(argv)
  sourceFileList = options.sourceFileList
  resultFile = options.resultFile
  kwAcDict = defaultdict(int)
  kwAcNum = 0
  for sourceFile in sourceFileList:
    sourceFd = codecs.open(sourceFile, mode = "r", encoding = "utf-8")
    sourceLineNum = 0
    for line in sourceFd:
      sourceLineNum += 1
      kwAcNum += 1
      kwAcList = line.strip().split("\t->\t")
      if len(kwAcList) < 2:
        print("parse error for file and  line ", sourceFile, sourceLineNum, line.strip("\n"))
        continue
      key = (kwAcList[-1], kwAcList[-2])
      kwAcDict[key] += 1
    sourceFd.close()
  uniqKwAcNum = len(kwAcDict)
  print("located {} unique ones from {} kw-ac pairs, the percentage is {:.2f}".format(uniqKwAcNum, kwAcNum, uniqKwAcNum / kwAcNum))
  if resultFile is None:
    return
  with codecs.open(resultFile, mode = "w", encoding = "utf-8") as resultFd:
    for kwAc in kwAcDict:
      count = kwAcDict[kwAc]
      resultList = list(kwAc) + [str(count)]
      resultFd.write("->".join(resultList) + "\n")

def lenDistribution(argv):
  parser = argparse.ArgumentParser()
  parser.add_argument("-sf", "--sourceFileList", nargs = "+", type = str)
  parser.add_argument("-rf", "--resultFile", type = str, default = None)
  options = parser.parse_args(argv)
  sourceFileList = options.sourceFileList
  resultFile = options.resultFile
  acDict = defaultdict(int)
  kwDict = defaultdict(int)
  kwAcNum = 0
  splitRe = re.compile("[ \t]+", re.I | re.U)
  for sourceFile in sourceFileList:
    sourceFd = codecs.open(sourceFile, mode = "r", encoding = "utf-8")
    sourceLineNum = 0
    for line in sourceFd:
      sourceLineNum += 1
      kwAcNum += 1
      kwAcList = line.strip().split("\t->\t")
      if len(kwAcList) < 2:
        print("parse error for file and  line ", sourceFile, sourceLineNum, line.strip("\n"))
        continue
      ac = kwAcList[-1]
      kw = kwAcList[-2]
      acWordList = splitRe.split(ac)
      kwWordList = splitRe.split(kw)
      kwDict[len(kwWordList)] += 1
      acDict[len(acWordList)] += 1
    sourceFd.close()
  acTupleList = []
  for length in acDict:
    count = acDict[length]
    acTupleList.append((length, count))
  sortedAcTupleList = sorted(acTupleList, key = lambda item : item[0])
  acDistList = []
  leftCount = kwAcNum
  for acTuple in sortedAcTupleList:
    length = acTuple[0]
    count = acTuple[1]
    leftCount -= count
    leftPercent = leftCount / kwAcNum
    print("ac length: {}, count: {}, percentage: {:.4f}".format(length, count,  leftPercent))
    acDistList.append((length, count, leftPercent)) 
  kwTupleList = []
  for length in kwDict:
    count = kwDict[length]
    kwTupleList.append((length, count))
  sortedKwTupleList = sorted(kwTupleList, key = lambda item : item[0])
  kwDistList = []
  leftCount = kwAcNum
  for kwTuple in sortedKwTupleList:
    length = kwTuple[0]
    count = kwTuple[1]
    leftCount -= count
    leftPercent = leftCount / kwAcNum
    print("kw length: {}, count: {}, percentage: {:.4f}".format(length, count, leftPercent))
    kwDistList.append((length, count, leftPercent)) 
  if resultFile is None:
    return
  with codecs.open(resultFile, mode = "w", encoding = "utf-8") as resultFd:
    for acDist in acDistList:
      length = acDist[0]
      count = acDist[1]
      dist = acDist[2]
      resultList = [str(length), str(count), str(dist)]
      resultFd.write("ac:" + ":".join(resultList) + "\n")
    for kwDist in kwDistList:
      length = kwDist[0]
      count = kwDist[1]
      dist = kwDist[2]
      resultList = [str(length), str(count), str(dist)]
      resultFd.write("kw:" + ":".join(resultList) + "\n")

def countUniqueWords(argv):
  parser = argparse.ArgumentParser()
  parser.add_argument("-sf", "--sourceFileList", nargs = "+", type = str)
  parser.add_argument("-rf", "--resultFile", type = str, default = None)
  options = parser.parse_args(argv)
  sourceFileList = options.sourceFileList
  resultFile = options.resultFile
  uniqWordSet = set()
  wordRe = re.compile("\w+", re.I | re.U)
  wordNum = 0
  for sourceFile in sourceFileList:
    sourceFd = codecs.open(sourceFile, mode = "r", encoding = "utf-8")
    sourceLineNum = 0
    for line in sourceFd:
      sourceLineNum += 1
      subWordSet = set(wordRe.findall(line.strip()))
      wordNum += len(subWordSet)
      uniqWordSet |= subWordSet
    sourceFd.close()
  print("got {} unique words out of {} words".format(len(uniqWordSet), wordNum))
  if resultFile is None:
    return

  with codecs.open(resultFile, mode = "w", encoding = "utf-8") as resultFd:
    for word in uniqWordSet:
      resultFd.write(word + "\n")


functionList = {
  "unique" : uniqKwAcPair,
  "length" : lenDistribution,
  "freq" : updateFrequency,
  "uniqueWords" : countUniqueWords,
}
if __name__ == "__main__": 
  funcName = sys.argv[1]
  if funcName not in functionList:
    print(" function name not found in list ", functionList.keys())
    sys.exit(1)
  function = functionList[funcName]
  function(sys.argv[2:])
