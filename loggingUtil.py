import time
import logging
import os,sys
import __main__ as main
def initLog():
  # -- Set Logger --
  mainFile = os.path.basename(main.__file__)
  logfile = time.strftime('%Y-%m-%d-%H', time.localtime()) + "-" + mainFile + '.log'
  logger = logging.getLogger('logger')
  formatter = logging.Formatter('%(processName)s-%(asctime)s - %(levelname)s - \
  [%(filename)s: %(lineno)s - %(funcName)s] %(message)s')
  logger.setLevel(logging.DEBUG)
  fh = logging.FileHandler(logfile)
  fh.setLevel(logging.DEBUG)
  fh.setFormatter(formatter)
  ch = logging.StreamHandler(sys.stdout)
  ch.setLevel(logging.DEBUG)
  ch.setFormatter(formatter)
  logger.addHandler(fh)
  logger.addHandler(ch)
  logger.info('Logger initialized success')
  return logger
