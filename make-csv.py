#!/usr/bin/python3
# -*- coding: utf-8 -*-
# vim: set fileencoding=UTF-8 :
"""
make-csv

Read Pure JSON data and create CSV file.

The flow of this script is as follows:
1. main      -- command line arguments, flow control
2. readjson  -- read data from json files, return data objects
4. output    -- write data to a CSV file

Note:
TODO? See that VIRTA section is okay in Pure.cfg

Possible subject for improvement:
  All JSON files are read into memory. This is something that
  could easily be changed if there appears any performance issues.
"""
import sys, getopt
import csv
import json
import configparser

def output(outputfile,jsondata,verbose):
  #jsondata = map(lambda x: flattenjson(x, "_"), jsondata)
  # and find the relevant column names:
  columns = [ x for row in jsondata for x in row.keys() ]
  columns = list(set(columns))

  # write to outputfile (always)
  with open(outputfile, 'w', newline='', encoding="UTF-8") as f:
    #writer = csv.DictWriter(f, fieldnames=csvdata[0], delimiter=';', quotechar='"', extrasaction='ignore')
    writer = csv.DictWriter(f, fieldnames=columns, delimiter=';', quotechar='"', extrasaction='ignore')
    writer.writeheader()
    #writer = csv.writer(f, delimiter=';', quotechar='"')
    #writer.writerow(columns)
    count=0
    for row in jsondata:
      count+=1
      if verbose>2: print("Output CSV (%s) with row: %s"%(outputfile,row,))
      writer.writerow(row)
      #writer.writerow(map(lambda x: row.get(x, ""), columns))

  if verbose: print("Output written to file '%s' with %d rows"%(outputfile,count,))

# Credits: https://stackoverflow.com/a/28246154
def flattenjson(b, delim):
  val = {}
  for i in b.keys():
    if isinstance(b[i], dict):
      get = flattenjson(b[i], delim)
      for j in get.keys():
        val[ i + delim + j ] = get[j]
    # my extra if list type with one value
    elif isinstance(b[i], list) and len(b[i]) == 1:
      if isinstance(b[i][0], dict):
        get = flattenjson(b[i][0], delim)
        for j in get.keys():
          val[ i + delim + j ] = get[j]
      else:
        val[i] = b[i][0]
    # / my extra
    else:
      val[i] = b[i]

  return val

def readjson(file,verbose):
  if verbose: print("Read JSON from '%s'"%(file,))
  items = []
  with open(file, 'rb') as f:
    rawjson = json.load(f)
    if verbose>2: print("%s"%(rawjson,))
    for rawitem in rawjson["items"]:
      # apply flatten to each dict in the input array of JSON objects:
      jsonitem = flattenjson(rawitem,"_")
      items.append(jsonitem)
      
  return items

def usage():
  print("""usage: make-csv.py [OPTIONS]

OPTIONS
-h, --help          : this message and exit
-L, --locale <loc>  : locale to filter data

Source files with defaults from configuration:
-i, --input <file>

Output file with default from configuration:
-o, --output <file>

-v, --verbose       : increase verbosity
-q, --quiet         : reduce verbosity
""")

def main(argv):
  cfgsec = "VIRTA"
  cfg = configparser.ConfigParser()
  cfg.read('Pure.cfg')
  if not cfg.has_section(cfgsec):
    print("Failed reading config. Exit")
    exit(1)
  # continue w/ [cfgsec] config

  # default/configuration values
  locale = None
  verbose = 1 # default minor messages
  inputfile = cfg.get(cfgsec,"researchfile") if cfg.has_option(cfgsec,"researchfile") else None
  outputfile = cfg.get(cfgsec,"outputfile") if cfg.has_option(cfgsec,"outputfile") else None

  # read possible arguments. all optional given that defaults suffice
  try:
    opts, args = getopt.getopt(argv,"hL:i:o:vq",["help","locale=","input=","output=","verbose","quiet"])
  except getopt.GetoptError as err:
    print(err)
    sys.exit(2)
  for opt, arg in opts:
    if opt in ("-h", "--help"):
      usage()
      sys.exit(0)
    elif opt in ("-L", "--locale"): locale = arg
    elif opt in ("-i", "--input"): inputfile = arg
    elif opt in ("-o", "--output"): outputfile = arg
    elif opt in ("-v", "--verbose"): verbose += 1
    elif opt in ("-q", "--quiet"): verbose -= 1

  if not inputfile: exit("No input file. Exit.")
  if not outputfile: exit("No output file. Exit.")

  items = readjson(inputfile,verbose)
  output(outputfile,items,verbose)
  
if __name__ == "__main__":
  main(sys.argv[1:])
