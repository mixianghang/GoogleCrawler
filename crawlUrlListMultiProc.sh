#!/bin/bash
function checkError() {
  if [ $? -ne 0 ];then
    echo "error when $1"
	exit 1
  fi
}
if [ $# -lt 2 ];then
  echo "Usage sourceUrlList, resultDir"
  exit 1
fi
urlList=$1
resultDir=$2
if [ ! -e $resultDir ];then
  mkdir -p $resultDir
fi
numIteration=50
if [ $# -ge 3 ];then
  numIteration=$3
fi
processNum=15
threadNum=10
if [ $# -ge 5 ];then
  processNum=$4
  threadNum=$5
fi

baseDir=/home/xmi/iotcommsec/crawlers/smartCrawler/googleCrawler

tryNum=0
currentUrlList=$urlList
startTime=$(date +"%s")
while [ $tryNum -lt 2 ];#retry failed requests based on retryList
do
  echo "params: processNum $processNum, threadNum $threadNum, numOfIteration $numIteration"
  echo "params: sourceUrlList $urlList, resultDir $resultDir"
  echo "start to filter and split urllist"
  subUrlDir=$resultDir/splitUrls_$retryNum
  $baseDir/filterAndSplitEntryUrlList.py $currentUrlList -f -s -sn $numIteration $subUrlDir
  checkError "filter and split urllist"

  round=0
  while [ $round -lt $numIteration ];
  do
	echo "current round is $round"
	$baseDir/crawlUrlListMultiProc.py  -s -st -th $threadNum -p $processNum $subUrlDir/urlPart_$round \
	$resultDir
	checkError "crawling for round $round"
	successNum=$(wc -l <$resultDir/UrlQueryResultList)
	retryNum=$(wc -l <$resultDir/retryUrlList)
	echo "current success $successNum, retryNum $retryNum"
	((round++))
  done
  successNum=$(wc -l <$resultDir/UrlQueryResultList)
  retryNum=$(wc -l <$resultDir/retryUrlList)
  echo "round $tryNum is finished with success $successNum and retryNum $retryNum"
  if [ $retryNum -le 0 ];then
	break
  fi
  if [ $retryNum -le $numIteration ];then
	numIteration=1
	$processNum=1
	if [ $retryNum -le $threadNum ];then
	  $threadNum = $retryNum
	fi
  fi
  mv $resultDir/retryUrlList $resultDir/retryUrlList_$tryNum
  currentUrlList=$resultDir/retryUrlList_$tryNum
  sleep 5
  ((tryNum++))
done
endTime=$(date +"%s")
echo "time cost is $(($endTime - $startTime)) seconds"
