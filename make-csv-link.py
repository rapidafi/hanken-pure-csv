#!/usr/bin/python3
# -*- coding: utf-8 -*-
# vim: set fileencoding=UTF-8 :
"""
make-csv-link

Read Pure JSON data and create CSV file that links two sets.

The flow of this script is as follows:
1. main      -- command line arguments, flow control
2. readjson  -- read data from json files, return a list of flat json objects
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
  # find the relevant column names:
  columns = [ x for row in jsondata for x in row.keys() ]
  columns = list(set(columns))

  # write to outputfile (always)
  with open(outputfile, 'w', newline='', encoding="UTF-8") as f:
    writer = csv.DictWriter(f, fieldnames=columns, delimiter=';', quotechar='"', extrasaction='ignore')
    writer.writeheader()
    count=0
    for row in jsondata:
      count+=1
      if verbose>2: print("Output CSV (%s) with row: %s"%(outputfile,row,))
      writer.writerow(row)

  if verbose: print("Output written to file '%s' with %d rows"%(outputfile,count,))

def readjson(file,link1,link2,verbose):
  if verbose: print("Read JSON from '%s'"%(file,))
  items = []
  allcount=0
  with open(file, 'rb') as f:
    rawjson = json.load(f)
    if verbose>2: print("%s"%(rawjson,))
    for row in rawjson["items"]:
      allcount+=1
      if verbose>1: print("%s start %d"%(row["uuid"],allcount,))
      
      # research-output and organisation
      # nb! only one organisation per research-output
      if link1=="research-output" and link2=="organisation":
        # make this a row in final set already
        items.append({"research_output_uuid": row["uuid"], "managingOrganisationalUnit_uuid": row["managingOrganisationalUnit"]["uuid"]})
      #/

      # research-output and person
      if link1=='research-output' and link2=='person':
        authors = []
        for r in row["personAssociations"]:
          #test for role
          roleIsOK = False
          for rr in r["personRole"]:
            # nb! authors and editors
            if rr["value"] == "Author" or rr["value"] == "Editor":
              roleIsOK = True
          #test for name:
          nameIsOK = False
          if "name" in r:
            if "lastName" in r["name"]:
              if "firstName" in r["name"]:
                nameIsOK = True
          if roleIsOK and nameIsOK:
            author = {
              "research_output_uuid": row["uuid"],
              "person_uuid": r["person"]["uuid"] if "person" in r else None, # may be empty (if external)
              "external_person_uuid": r["externalPerson"]["uuid"] if "externalPerson" in r else None, # may be empty
              "author_name": r["name"]["lastName"].strip()+", "+r["name"]["firstName"].strip(),
              #"last": r["name"]["lastName"].strip(), #extra
              #"first": r["name"]["firstName"].strip() #extra
            }
            # author done (make this a row in final set)
            items.append(author)
      #/
      
  if verbose: print("There were %d objects in total"%(allcount,))

  return items

def usage():
  print("""usage: make-csv-link.py [OPTIONS]

OPTIONS
-h, --help          : this message and exit
-L, --locale <loc>  : locale to filter data

Source files with defaults from configuration:
-i, --input <file>

Output file with default to a dynamic value (two linked set names):
-o, --output <file>

Which datasets will be linked with defaults:
-a, --link1 <set>   : "research-output" (no other valid values)
-b, --link2 <set>   : "person" (other valid values: organisation)

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
  outputfile = "link.csv" # altered to a dynamic value if not given via arguments
  link1 = "research-output" # link between person and orgnisation from another source, perhaps? is there any other linkage needed than those starting from research-output?
  link2 = "person" # organisation, journals?

  # read possible arguments. all optional given that defaults suffice
  try:
    opts, args = getopt.getopt(argv,"hL:i:o:a:b:vq",["help","locale=","input=","output=","link1=","link2=","verbose","quiet"])
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
    elif opt in ("-a", "--link1"): link1 = arg
    elif opt in ("-b", "--link2"): link2 = arg
    elif opt in ("-v", "--verbose"): verbose += 1
    elif opt in ("-q", "--quiet"): verbose -= 1

  # fix outputfile if not given
  if outputfile=="link.csv":
    outputfile = link1+"-"+link2+".csv"

  if not inputfile: exit("No input file. Exit.")
  if not outputfile: exit("No output file. Exit.")
  if not link1: exit("No link1 dataset. Exit.")
  if not link2: exit("No link2 dataset. Exit.")

  items = readjson(inputfile,link1,link2,verbose)
  output(outputfile,items,verbose)
  
if __name__ == "__main__":
  main(sys.argv[1:])
