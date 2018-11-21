#!/usr/bin/python
from stem.control import Controller
from stem import Signal
import stem
import stem.connection
import requesocks
from subprocess import call
def getSession():
  mainsession = requesocks.session()
  mainsession.proxies = {'http':  'socks5://127.0.0.1:9050', 'https': 'socks5://127.0.0.1:9050'}
  return mainsession
def renewConn():
  try:
    print "start to connect to controller"
    controller = Controller.from_port()
  except stem.SocketError as exc:
    print("Unable to connect to tor on port 9051: %s" % exc)
    return -1

  try:
    controller.authenticate(password="password")
  except stem.connection.MissingPassword:
    try:
      controller.authenticate(password = "password")
    except stem.connection.PasswordAuthFailed:
      print("Unable to authenticate, password is incorrect")
      return -1
  except stem.connection.AuthenticationFailure as exc:
    print("Unable to authenticate: %s" % exc)
    return -1
  except Exception  as e:
    return -1

  print("Tor is running version %s" % controller.get_version())
  retry = 0
  while True:
      if controller.is_newnym_available():
          print "tor can accept newnym signal"
          break
      else:
          if retry >= 3:
              print "tor cannot accept newnym signal"
              return -1
          retry += 1
          time.sleep(10)
  try:
    controller.signal(Signal.NEWNYM)
  except Exception as e:
    error(repr(e))
    return -1
  print "send renew connection signal success"
  controller.close() 
  return 0
