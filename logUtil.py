#!/usr/bin/python
import time
import os, sys
import logging
def initLog(logDir, functionName = None, isStd = False, isFile = False, name = "main logger"):
  mainFile = os.path.basename(__file__)
  if functionName:
    logfile = time.strftime('%Y-%m-%d-%H-%M-%S', time.localtime()) + "-" + functionName + '.log'
  else:
    logfile = time.strftime('%Y-%m-%d-%H-%M-%S', time.localtime()) + "-" + mainFile + '.log'
  logfile = os.path.join(logDir, logfile)
  logger = logging.getLogger(name)
  formatter = logging.Formatter('%(processName)s_%(threadName)s_%(asctime)s %(name)s %(levelname)s-[%(filename)s: %(lineno)s - %(funcName)s] %(message)s')
  logger.setLevel(logging.DEBUG)
  fh = logging.FileHandler(logfile)
  fh.setLevel(logging.DEBUG)
  fh.setFormatter(formatter)
  logger.addHandler(fh)
  if isStd:
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
  logger.info('Logger initialized success nihao')
  return logger
