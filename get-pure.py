#!/usr/bin/python3
# -*- coding: utf-8 -*-
# vim: set fileencoding=UTF-8 :
"""
get-pure

Read data from API.
"""
import os, sys, getopt
import requests
import json
from time import localtime, strftime

import configparser
cfgsec = "API"
cfg = configparser.ConfigParser()
cfg.read('Pure.cfg')
if not cfg.has_section(cfgsec):
  print("Failed reading config. Exit")
  exit(1)
# continue w/ [cfgsec] config

apihost = cfg.get(cfgsec,"hostname") if cfg.has_option(cfgsec,"hostname") else None
apiuri = cfg.get(cfgsec,"uri") if cfg.has_option(cfgsec,"uri") else None
apiuser = cfg.get(cfgsec,"username") if cfg.has_option(cfgsec,"username") else exit("No username in config. Exit.")
apipass = cfg.get(cfgsec,"password") if cfg.has_option(cfgsec,"password") else exit("No password in config. Exit.")
apikey = cfg.get(cfgsec,"apikey") if cfg.has_option(cfgsec,"apikey") else exit("No apikey in config. Exit.")


def show(message):
  print(strftime("%Y-%m-%d %H:%M:%S", localtime())+" "+message)

def load(secure,hostname,uri,api,locale,output,size,split,verbose):
  global apiuser, apipass, apikey
  if verbose: show("begin")

  # REQUESTS
  # nb! could use requests.get *params* but since
  #     Pure API provides navigation links with params (full URI)
  #     we produce similar URI to begin with
  requri = 'https://%s%s/%s'%(hostname,uri,api,)
  requri += '?navigationLink=true&size=%d&offset=%d'%(size,0,)
  if locale:
    requri += '&locale=%s'%(locale,)
  reqheaders = {'Accept': 'application/json'}
  reqheaders['api-key'] = apikey    
  
  fullset = json.loads('{"items":[]}')
  thereismore = True # load until theres no more left (via navigationLink.href)
  index = 0 # increment immediately
  cnt = 0
  while thereismore:
    thereismore = False # now assume no more, figure out at the end of loop
    index += 1

    try:
      if verbose>1: show("call: "+requri)
      r = requests.get(requri, headers=reqheaders, auth=(apiuser, apipass))
    except requests.exceptions.RequestException as e:
      print(e)
      print(requests)
      sys.exit(1)

    if r.status_code != 200:
      print("Error! HTTP status code: " + str(r.status_code))
      sys.exit(2)

    try:
      result = json.loads(r.content)
      if output:
        fullset["items"] += result["items"]
        with open(output, "w") as f:
          json.dump(fullset, f)
        if split: # special case
          (pre,ext) = output.split(".", -1)
          outputfile = (pre+"-{:04d}."+ext).format(index,)
          if verbose: show("saving to "+outputfile)
          with open(outputfile, "wb") as f:
            f.write(r.content)
    except ValueError as e:
      print(e)
      sys.exit(3)

    #show(str(result["count"]))
    cnt+=len(result["items"])
    if verbose: show("index: "+str(index)+" with "+str(cnt)+" items (total "+str(result["count"])+")")

    # keep loading?
    if "navigationLinks" in result:
      for nav in result["navigationLinks"]: # an array for prev/next!
        if "ref" in nav and "href" in nav:
          if nav["ref"] == "next":
            requri = nav["href"]
            thereismore = True
  
  if verbose and output:
    show("wrote %d items to %s"%(cnt,output,))

  if verbose: show("ready")

def usage():
  print("""usage: get-pure.py [OPTIONS] <API>

<API> is the name of the Pure API to get data from

OPTIONS
-h, --help          : this message and exit
-H, --host <host>   : hostname of Pure API
                      defaults to configuration value
                      then to $PURE_HOSTNAME
-u, --uri <uri>     : base part of URI of Pure API
                      defaults to configuration value
                      then to $PURE_URI
-L, --locale <loc>  : locale to use to filter data from API
                      defaults to en_GB
-s, --size <size>   : number of items to limit the query size of API
                      defaults to 1000 (API default is 10)
-o, --output <file> : filename to write output to
                      may result to many files if -S is given
                      with pattern like "api.json" => "api-0001.json"
                      defaults to "<API>.json"
-S, --split         : split files with max <size> entries each
-v, --verbose       : increase verbosity
-q, --quiet         : reduce verbosity
""")

def main(argv):
  global apihost, apiuri
  # variables from arguments with possible defaults
  secure = True # always secure, so not even argumented anymore!
  hostname = apihost or os.getenv("PURE_HOSTNAME")
  uri = apiuri or os.getenv("PURE_URI")
  api = None
  locale = 'en_GB'
  size = 1000
  output = None
  split = False
  verbose = 1 # default minor messages

  try:
    opts, args = getopt.getopt(argv,"hH:u:L:o:s:Svq",["help","host=","uri=","locale=","output=","size=","split","verbose","quiet"])
  except getopt.GetoptError as err:
    print(err)
    usage()
    sys.exit(2)
  for arg in args:
    api = arg
  for opt, arg in opts:
    if opt in ("-h", "--help"):
      usage()
      sys.exit(0)
    elif opt in ("-H", "--host"): hostname = arg
    elif opt in ("-u", "--uri"): uri = arg
    elif opt in ("-L", "--locale"): locale = arg
    elif opt in ("-o", "--output"): output = arg
    elif opt in ("-s", "--size"): size = int(arg)
    elif opt in ("-S", "--split"): split = True
    elif opt in ("-v", "--verbose"): verbose += 1
    elif opt in ("-q", "--quiet"): verbose -= 1

  if not hostname: exit("No hostname configured or given. Exit.")
  if not uri: exit("No URI configured or given. Exit.")

  if not api:
    usage()
    sys.exit(2)

  if not output:
    output = api+".json"

  load(secure,hostname,uri,api,locale,output,size,split,verbose)

if __name__ == "__main__":
  main(sys.argv[1:])
