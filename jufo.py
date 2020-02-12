#!/usr/bin/python3
# -*- coding: utf-8 -*-
# vim: set fileencoding=UTF-8 :
"""
jufo

Module to read data from JUFO REST API.
"""
import os, sys, getopt
import requests
import json
from time import localtime, strftime

import configparser
cfgsec = "API"
cfg = configparser.ConfigParser()
cfg.read('Jufo.cfg')
if not cfg.has_section(cfgsec):
  print("Failed reading config. Exit")
  exit(1)
# continue w/ [cfgsec] config

apihost = cfg.get(cfgsec,"hostname") if cfg.has_option(cfgsec,"hostname") else None
apiuri = cfg.get(cfgsec,"uri") if cfg.has_option(cfgsec,"uri") else None

def show(message):
  print(strftime("%Y-%m-%d %H:%M:%S", localtime())+" "+message)

def get(code,verbose=0):
  global apihost, apiuri
  if verbose: show("begin")
  if not code: return

  jufodata = None

  # try to read from already loaded file
  filename = "jufo_%s.json"%(code,)
  if os.path.exists(filename):
    with open(filename, "r") as f:
      jufodata = json.load(f)

  if jufodata:
    if verbose: show("%s read from file"%(code,))
    return jufodata

  # load from API if no file was found

  # REQUESTS
  # nb! could use requests.get *params* but since
  #     Pure API provides navigation links with params (full URI)
  #     we produce similar URI to begin with
  requri = 'https://%s%s/%s'%(apihost,apiuri,code,)
  reqheaders = {'Accept': 'application/json'}
  
  try:
    if verbose>1: show("call: "+requri)
    r = requests.get(requri, headers=reqheaders)
  except requests.exceptions.RequestException as e:
    print(e)
    print(requests)
    sys.exit(1)

  if r.status_code != 200:
    print("Error! HTTP status code: " + str(r.status_code))
    sys.exit(2)

  try:
    result = json.loads(r.content)
  except ValueError as e:
    print(e)
    sys.exit(3)

  if verbose>1: show(result[0]["Jufo_ID"])

  if verbose: show("ready")
  return result

def usage():
  print("""usage: jufo.py [OPTIONS] <CODE>

<CODE> is the Jufo_ID to get data from

OPTIONS
-h, --help          : this message and exit
-H, --host <host>   : hostname of API
                      defaults to configuration value
-u, --uri <uri>     : base part of URI of Pure API
                      defaults to configuration value
-o, --output <file> : filename to write output to
                      defaults to "jufo_<CODE>.json"
-v, --verbose       : increase verbosity
-q, --quiet         : reduce verbosity

Or use as a module with: jsondata = jufo.get(<CODE>)
""")

def main(argv):
  # variables from arguments with possible defaults
  code = None
  output = None
  split = False
  verbose = 1 # default minor messages

  try:
    opts, args = getopt.getopt(argv,"ho:vq",["help","output=","verbose","quiet"])
  except getopt.GetoptError as err:
    print(err)
    usage()
    sys.exit(2)
  for arg in args:
    code = arg
  for opt, arg in opts:
    if opt in ("-h", "--help"):
      usage()
      sys.exit(0)
    elif opt in ("-o", "--output"): output = arg
    elif opt in ("-v", "--verbose"): verbose += 1
    elif opt in ("-q", "--quiet"): verbose -= 1

  if not code:
    usage()
    sys.exit(2)

  jufodata = get(code,verbose)

  if output:
    with open(output, "w") as f:
      json.dump(jufodata, f)
  if verbose and output:
    show("wrote to %s"%(output,))

if __name__ == "__main__":
  main(sys.argv[1:])
