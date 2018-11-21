#!/usr/bin/python
import requests
from lxml import etree
import os, sys
import logUtil
import argparse
import codecs
import json
import re, time, random
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

logger = logUtil.initLog("./", isStd = True)
headers = {
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, sdch, br',
    'Accept-Language': 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4',
}

from threading import Thread, Lock
class ProxyThread(Thread):
  def __init__(self, baseDir, updateInterval = 62):
    self.updateInterval = updateInterval
    self.baseDir = baseDir
    self.isReady = False
    if not os.path.exists(self.baseDir):
      os.makedirs(self.baseDir)
    self.proxyList = None
    self.isStop = False
    super(ProxyThread, self).__init__()

  def isProxyReady(self):
    return self.isReady
  def logProxyList(self):
    proxyFileName = "proxyList_" + time.strftime("%Y%m%d_%H%M%S")
    with open(os.path.join(self.baseDir, proxyFileName), "w") as fd:
      for proxy in self.proxyList:
        fd.write(json.dumps(proxy) + "\n")

  def getRandomProxy(self, subTypeList = None, latencyLimit = None):
    qualifiedProxyList = []
    if subTypeList is None and latencyLimit is None:
      qualifiedProxyList = self.proxyList
    elif latencyLimit is None:
      for proxy in self.proxyList:
        typeList = proxy["typeList"]
        latency = proxy["speedInMS"]
        for type in subTypeList:
          if type in typeList:
            qualifiedProxyList.append(proxy)
    elif subTypeList is None:
      for proxy in self.proxyList:
        typeList = proxy["typeList"]
        latency = proxy["speedInMS"]
        for type in subTypeList:
          if latency <= latencyLimit:
            qualifiedProxyList.append(proxy)
    else:
      for proxy in self.proxyList:
        typeList = proxy["typeList"]
        latency = proxy["speedInMS"]
        if latency <= latencyLimit:
          for type in subTypeList:
            if type in typeList:
              qualifiedProxyList.append(proxy)
    if len(qualifiedProxyList) == 0:
      return None
    print("qualified proxy number is {0}".format(len(qualifiedProxyList)))
    index = random.randint(0, len(qualifiedProxyList) - 1)
    return qualifiedProxyList[index]
    
  def run(self):
    while True:
      if self.isStop:
        return
      print("start to crawl proxyList")
      self.proxyList = crawlProxyList()
      print("finish crawling proxyList with {0} proxiles".format(len(self.proxyList)))
      self.isReady = True
      self.logProxyList()
      time.sleep(self.updateInterval)
      
  def stopProxy(self):
    self.isStop = True
  def getProxyNum(self):
    return len(self.proxyList)

def crawlProxyList(resultDir = None):
  proxyList = [] # each item is ip, port, support protocols(1 http, 2 https 4 sock4 8 sock5), speed, anonymity
  #crawl from  https://incloak.com/proxy-list/
  timeout = (10, 60)
  verify = False
  #subDir = os.path.join(resultDir, "incloak")
  #if not os.path.exists(subDir):
  #  os.makedirs(subDir)
  startIndex = 0
  baseUrl = "https://incloak.com/proxy-list/"
  ipPath = "//table[@class='proxy__t']/tbody/tr/td[1]/text()" 
  portPath = "//table[@class='proxy__t']/tbody/tr/td[2]/text()" 
  countryPath = "//table[@class='proxy__t']/tbody/tr/td[3]/div/text()"
  speedPath = "//table[@class='proxy__t']/tbody/tr/td[4]/div/div/p/text()"
  typePath = "//table[@class='proxy__t']/tbody/tr/td[5]/text()"
  anonymityPath = "//table[@class='proxy__t']/tbody/tr/td[6]/text()"
  speedRe = re.compile("(\d+)", re.I | re.U)
  while (True): 
    requestUrl = baseUrl + "?start={0}".format(startIndex)
    response = requests.get(requestUrl, timeout = timeout, verify = False, headers = headers)
    #with codecs.open(os.path.join(subDir, "incloak_{0}".format(startIndex)), mode = "w", encoding = "utf-8") as fd:
    #  fd.write(response.text)
    if response.status_code != 200:
      logger.error("request from %s failed with %d", requestUrl, response.status_code)
      break
    tree = etree.HTML(response.text)
    ipSubList = tree.xpath(ipPath)
    portSubList = tree.xpath(portPath)
    countrySubList = tree.xpath(countryPath)
    speedSubList = tree.xpath(speedPath)
    typeSubList = tree.xpath(typePath)
    anonymitySubList = tree.xpath(anonymityPath)
    if len(ipSubList) <= 0:
      break
    for i in range(len(ipSubList)):
      ip = ipSubList[i]
      port = int(portSubList[i])
      country = countrySubList[i].strip()
      speed = int(speedRe.search(speedSubList[i]).group(1))
      typeList = typeSubList[i].split(", ")
      anonymity = anonymitySubList[i]
      proxyItem = {
        "ip" : ip,
        "port" : port,
        "country" : country,
        "speedInMS" : speed,
        "typeList" : typeList,
        "anonymity" : anonymity,
      }
      proxyList.append(proxyItem)
    startIndex += len(ipSubList)
  logger.debug("Got %d ip:port pairs from list %s", startIndex, baseUrl)

  startIndex = 0
  startPage = 1
  baseUrl = "http://proxylist.hidemyass.com/"
  ipPath = "//table/tbody/tr/td[2]/span[1]/text()" 
  portPath = "//table/tbody/tr/td[3]/text()" 
  countryPath = "//table/tbody/tr/td[4]/@rel"
  speedPath = "//table/tbody/tr/td[5]/div/@value"
  typePath = "//table/tbody/tr/td[6]/text()"
  anonymityPath = "//table/tbody/tr/td[7]/text()"
  speedRe = re.compile("(\d+)", re.I | re.U)
  while (False): 
    requestUrl = baseUrl + "".format(startPage)
    response = requests.get(requestUrl, timeout = timeout, verify = False, headers = headers)
    #with codecs.open(os.path.join(subDir, "incloak_{0}".format(startIndex)), mode = "w", encoding = "utf-8") as fd:
    #  fd.write(response.text)
    if response.status_code != 200:
      logger.error("request from %s failed with %d", requestUrl, response.status_code)
      break
    tree = etree.HTML(response.text)
    ipSubList = tree.xpath(ipPath)
    portSubList = tree.xpath(portPath)
    countrySubList = tree.xpath(countryPath)
    speedSubList = tree.xpath(speedPath)
    typeSubList = tree.xpath(typePath)
    anonymitySubList = tree.xpath(anonymityPath)
    print(ipSubList)
    print("number of different parts {} {} {} {}".format(len(ipSubList), len(portSubList), len(countrySubList),len(speedSubList)))
    if len(ipSubList) <= 0:
      break
    for i in range(len(ipSubList)):
      ip = ipSubList[i]
      port = int(portSubList[i])
      country = countrySubList[i].strip()
      speed = int(speedRe.search(speedSubList[i]).group(1))
      typeList = typeSubList[i].split(", ")
      anonymity = anonymitySubList[i]
      proxyItem = {
        "ip" : ip,
        "port" : port,
        "country" : country,
        "speedInMS" : speed,
        "typeList" : typeList,
        "anonymity" : anonymity,
      }
      proxyList.append(proxyItem)
    if len(ipSubList) == 0:
      break
    else:
      startPage += 1
    startIndex += len(ipSubList)
  logger.debug("Got %d ip:port pairs from url %s", startIndex, baseUrl)
  if resultDir is not None:
    with open(os.path.join(resultDir, "proxyList"), "w") as fd:
      for proxy in proxyList: 
        fd.write(json.dumps(proxy) + "\n")
  return proxyList


if __name__ == "__main__" :    
  parser = argparse.ArgumentParser("cralwProxyList")
  parser.add_argument("resultDir", type = str)
  options = parser.parse_args()
  if not os.path.exists(options.resultDir):
    os.makedirs(options.resultDir)
  proxyThread = ProxyThread(options.resultDir, updateInterval = 20)
  num = 5
  proxyThread.start()
  while num > 0 :
    num = num -  1
    time.sleep(30)
    logger.debug("current proxy number is %d", proxyThread.getProxyNum())
    logger.debug("random proxy is %s", proxyThread.getRandomProxy(subTypeList = ["HTTPS"], latencyLimit = 500))
  proxyThread.stopProxy()


