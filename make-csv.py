#!/usr/bin/python3
# -*- coding: utf-8 -*-
# vim: set fileencoding=UTF-8 :
"""
make-csv

Read Pure JSON data and create CSV file.

The flow of this script is as follows:
1. main      -- command line arguments, flow control
2. readjson  -- read data from json files, return data objects
             -- uses helper functions jv and getkeywordvalue
4. output    -- write data to a CSV file

Note:
TODO? See that CSV section is okay in Pure.cfg

Possible subject for improvement:
  All JSON files are read into memory. This is something that
  could easily be changed if there appears any performance issues.
"""
import sys, getopt
import csv
import json
import re
import configparser

from datetime import datetime
currentyear = datetime.now().year

def makerow(columns,verbose):
  rowheader = columns.copy()
  #rowheader.sort()
  rowheader = [
    "uuid",
    "electronicVersions_doi",
    "title",
    "abstract",
    "language",
    "type",
    "category",
    "assessmentType",
    "publicationStatuses_publicationDate_year",
    "publicationStatuses_current",
    "publicationStatuses_publicationStatus",
    "workflow",
    "totalNumberOfAuthors",
    "personAssociations_person_uuid",
    "personAssociations_name_lastName",
    "personAssociations_name_firstName",
    "personAssociations_personRole",
    "personAssociations_organisationalUnits_uuid",
    "personAssociations_organisationalUnits_name",
    "personAssociations_country",
    "personAssociations_externalPerson_uuid",
    "personAssociations_externalOrganisations_uuid",
    "personAssociations_externalOrganisations_name",
    "managingOrganisationalUnit_uuid",
    "managingOrganisationalUnit_name",
    "journalAssociation_issn",
    "journalAssociation_title",
    "journalAssociation_journal_type",
    "volume",
    "journalNumber",
    "pages",
    "articleNumber",
    "edition",
    "isbns",
    "openAccessPermission",
    "keyword_avoinsaatavuuskoodi",
    "keyword_JulkaisunKansainvalisyysKytkin",
    "keyword_KOTA",
    "keyword_rinnakkaistallennettukytkin",
    "keyword_YhteisjulkaisuKVKytkin",
    "keyword_YhteisjulkaisuYritysKytkin"
    #keyword_field511
    #keyword_field512
    #    etc
    ,"metrics_2014_citescore"
    ,"metrics_2014_sjr"
    ,"metrics_2014_snip"
    ,"metrics_2015_citescore"
    ,"metrics_2015_sjr"
    ,"metrics_2015_snip"
    ,"metrics_2016_citescore"
    ,"metrics_2016_sjr"
    ,"metrics_2016_snip"
    ,"metrics_2017_citescore"
    ,"metrics_2017_sjr"
    ,"metrics_2017_snip"
    ,"metrics_2018_citescore"
    ,"metrics_2018_sjr"
    ,"metrics_2018_snip"
  ]
  return rowheader

def output(outputfile,items,verbose):
  # find the column names:
  columns = [ x for row in items for x in row.keys() ]
  columns = list(set(columns))
  columns = makerow(columns,verbose)

  # write to outputfile (always)
  with open(outputfile, 'w', newline='', encoding="UTF-8") as f:
    writer = csv.DictWriter(f, fieldnames=columns, delimiter=';', quotechar='"', extrasaction='ignore')
    writer.writeheader()
    count=0
    for row in items:
      count+=1
      if verbose>2: print("Output CSV (%s) with row: %s"%(outputfile,row,))
      writer.writerow(row)

  if verbose: print("Output written to file '%s' with %d columns and %d rows"%(outputfile,len(columns),count,))

# Helper function for Haris/Pure JSON regarding keywords
def getkeywordvalue(keyword,item,verbose):
  (title,value) = (None,None)
  if "keywordGroups" in item:
    for k in item["keywordGroups"]:
      if "type" in k:
        for t in k["type"]:
          title = t["value"]
      if "keywords" in k:
        for w in k["keywords"]:
          if "uri" in w:
            # special case for "core"
            if keyword == "core" and "dk/atira/pure/core/keywords/" in w["uri"]:
              if "value" in w:
                value = w["value"]
                value = value.split(" ")[0]
                value = value.replace(",","") # remove comma "," if it exists, e.g. "612,1"->"6121"
                if re.search("^[0-9]+$", value):
                  if verbose>1: print("%s core keyword %s"%(item["uuid"],value,))
            elif "dk/atira/pure/keywords/"+keyword+"/" in w["uri"]:
              value = w["uri"].split("/")[-1] # last index
  
  return (title,value)

# Helper function for repeatedly used part of code
def jv(objectname,jsonitem):
  value = None
  if objectname in jsonitem:
    value = jsonitem[objectname]
  return value

# go thru given JSON. Look for bits were interested in and write to output file (CSV)
def parsejson(jsondata,keywords,metricdata,verbose):
  global currentyear

  items = []
  for j in jsondata:
    item = {}
    item["uuid"] = j["uuid"]

    electronicVersions_doi = None
    if "electronicVersions" in j:
      for a in j["electronicVersions"]:
        if "doi" in a:
          electronicVersions_doi = a["doi"]
    item["electronicVersions_doi"] = electronicVersions_doi

    item["title"] = jv("title",j)

    abstract = None
    if "abstract" in j:
      for a in j["abstract"]:
        abstract = a["value"]
    item["abstract"] = abstract

    language = None
    if "language" in j:
      for a in j["language"]:
        # nb! technically may be many but choose randomly last, then split with "_" i.e. "fi_FI" -> "fi"
        language = a["uri"].split("/")[-1].split("_")[0]
        # nb! there are some odd language values for ex. "/dk/atira/pure/core/languages/italian"?
        if   language=="chinese":        language = "zh"
        elif language=="italian":        language = "it"
        elif language=="polish":         language = "pl"
        elif language=="portuguese":     language = "pt"
        if language == "und": # value 99 is not used for unknown
          language = None
    item["language"] = language

    item["type"] = jv("type",j)[0]["value"] # only one or first will suffice

    category = None
    if "category" in j:
      for a in j["category"]:
        category = a["value"]
    item["category"] = category

    assessmentType = None
    if "assessmentType" in j:
      for a in j["assessmentType"]:
        assessmentType = a["value"]
    item["assessmentType"] = assessmentType

    publicationStatuses_publicationDate_year = None
    publicationStatuses_current = None
    publicationStatuses_publicationStatus = None
    if "publicationStatuses" in j:
      for a in j["publicationStatuses"]:
        publicationStatuses_publicationDate_year = a["publicationDate"]["year"]
        publicationStatuses_current = a["current"]
        if "publicationStatus" in a:
          for b in a["publicationStatus"]:
            publicationStatuses_publicationStatus = b["value"]
    item["publicationStatuses_publicationDate_year"] = publicationStatuses_publicationDate_year
    item["publicationStatuses_current"] = publicationStatuses_current
    item["publicationStatuses_publicationStatus"] = publicationStatuses_publicationStatus

    workflow = None
    if "workflow" in j:
      for a in j["workflow"]:
        workflow = a["value"]
    item["workflow"] = workflow

    item["totalNumberOfAuthors"] = str(jv("totalNumberOfAuthors",j))

    # nb! next would be personAssociation, but data addition done last, see below

    managingOrganisationalUnit_uuid = None
    managingOrganisationalUnit_name = None
    if "managingOrganisationalUnit" in j:
      managingOrganisationalUnit_uuid = j["managingOrganisationalUnit"]["uuid"]
      if "name" in j["managingOrganisationalUnit"]:
        for m in j["managingOrganisationalUnit"]["name"]:
          managingOrganisationalUnit_name = m["value"]
    item["managingOrganisationalUnit_uuid"] = managingOrganisationalUnit_uuid
    item["managingOrganisationalUnit_name"] = managingOrganisationalUnit_name

    journalAssociation_issn = None
    journalAssociation_title = None
    journalAssociation_journal_type = None
    # for scopus metrics fetching:
    journal_uuid = None
    if "journalAssociation" in j:
      a = j["journalAssociation"]
      if "issn" in a:
        journalAssociation_issn = a["issn"]["value"]
      if "title" in a:
        journalAssociation_title = a["title"]["value"]
      if "journal" in a:
        b = a["journal"]
        if "type" in b:
          for c in b["type"]:
            journalAssociation_journal_type = c["value"]
        if "uuid" in b:
          journal_uuid = b["uuid"]
    item["journalAssociation_issn"] = journalAssociation_issn
    item["journalAssociation_title"] = journalAssociation_title
    item["journalAssociation_journal_type"] = journalAssociation_journal_type

    item["volume"] = jv("volume",j)

    item["journalNumber"] = jv("journalNumber",j)

    item["pages"] = jv("pages",j)

    item["articleNumber"] = jv("articleNumber",j)

    item["edition"] = jv("edition",j)

    item["isbns"] = "" # nb! different from others!
    allisbns = []
    if "isbns" in j:
      allisbns += j["isbns"] # list+list
    if "electronicIsbns" in j:
      allisbns += j["electronicIsbns"] # list+list
    if len(allisbns)>0:
      isbncount=0
      for isbn in allisbns:
        # nb! ISSN and ISBN are mixed in data, separate: ISBN format is loose but at least 10 chars
        if len(isbn)>=10:
          isbncount+=1
          if isbncount > 1:
            item["isbns"] += ","
          item["isbns"] += isbn.strip()

    openAccessPermission = None
    if "openAccessPermission" in j:
      for a in j["openAccessPermission"]:
        openAccessPermission = a["value"]
    item["openAccessPermission"] = openAccessPermission

    # keywords pivot
    # - to title: keywordGroups.keywords.value => compromise this with setting value since there is no guarantee a keyword exists for all research-outputs
    # - to value: keywordGroups.type.value
    for k in keywords:
      (keyword_title,keyword_value) = getkeywordvalue(k,j,verbose)
      item["keyword_"+k] = keyword_value

    # TODO core keywords (tieteenalakoodit)

    # get scopusMetrics from journals
    metrics = ["sjr","snip","citescore"] # to config?
    years = 5 # to config?
    for m in metrics:
      for y in range(currentyear-years, currentyear+1):
        mkey = str(y)+"_"+m
        # this will make sure column exists in every row
        item["metrics_"+mkey] = None
        # get the actual value if exists
        if journal_uuid in metricdata:
          if mkey in metricdata[journal_uuid]:
            item["metrics_"+mkey] = metricdata[journal_uuid][mkey]

    #item["totalScopusCitations"] = jv("totalScopusCitations",j)
    
    # nb! row multiplying data
    # so do this/these last

    # multiply rows per person!
    added_persons = False # keep track if nothing was added
    if "personAssociations" in j:
      for a in j["personAssociations"]:
        added_persons = True # .. will be added
        # reset values here
        personAssociations_country_value = ""
        personAssociations_externalOrganisations_name_value = None
        personAssociations_externalOrganisations_uuid = None
        personAssociations_name_firstName = None
        personAssociations_name_lastName = None
        personAssociations_organisationalUnits_name_value = None
        personAssociations_organisationalUnits_uuid = None
        personAssociations_person_uuid = ""
        personAssociations_externalPerson_uuid = ""
        personAssociations_personRole_value = None
        if "country" in a:
          for b in a["country"]:
            personAssociations_country_value = b["value"]
        if "externalOrganisations" in a:
          for b in a["externalOrganisations"]:
            if "name" in b:
              for c in b["name"]:
                personAssociations_externalOrganisations_name_value = c["value"]
            personAssociations_externalOrganisations_uuid = b["uuid"]
        if "name" in a:
          personAssociations_name_firstName = a["name"]["firstName"]
          personAssociations_name_lastName = a["name"]["lastName"]
        if "organisationalUnits" in a:
          for b in a["organisationalUnits"]:
            if "name" in b:
              for c in b["name"]:
                personAssociations_organisationalUnits_name_value = c["value"]
            personAssociations_organisationalUnits_uuid = b["uuid"]
        if "person" in a:
          personAssociations_person_uuid = a["person"]["uuid"]
        if "externalPerson" in a:
          personAssociations_externalPerson_uuid = a["externalPerson"]["uuid"]
        if "personRole" in a:
          for b in a["personRole"]:
            personAssociations_personRole_value = b["value"]
        # add person values to item here, overwrite if 1+ round
        item["personAssociations_country_value"] = personAssociations_country_value
        item["personAssociations_externalOrganisations_name_value"] = personAssociations_externalOrganisations_name_value
        item["personAssociations_externalOrganisations_uuid"] = personAssociations_externalOrganisations_uuid
        item["personAssociations_name_firstName"] = personAssociations_name_firstName
        item["personAssociations_name_lastName"] = personAssociations_name_lastName
        item["personAssociations_organisationalUnits_name_value"] = personAssociations_organisationalUnits_name_value
        item["personAssociations_organisationalUnits_uuid"] = personAssociations_organisationalUnits_uuid
        item["personAssociations_person_uuid"] = personAssociations_person_uuid
        item["personAssociations_externalPerson_uuid"] = personAssociations_externalPerson_uuid
        item["personAssociations_personRole_value"] = personAssociations_personRole_value
        # and append here (not at "root" loop end)
        items.append(item.copy()) #nb! make a copy (not reference)
    # nb! normal addition for person (and such) would be at this level

    # normal "root" loop ending begins.
    # would normally append here in all cases but person multiplying makes this special
    # if no person was found then append here
    if not added_persons:
      if verbose: print("No person for: %s"&(item["uuid"],))
      items.append(item.copy()) #nb! make a copy (not reference)

  return items

def parsemetrics(journaldata,verbose):
  global currentyear
  metrics = ["sjr","snip","citescore"] # to config?
  years = 5 # to config?

  fromyear = currentyear-years
  if verbose>1: print("Parse metrics from year %d to %d"%(fromyear,currentyear-1,))

  metricdata = {}
  for jo in journaldata:
    if verbose>2: print("  >>> metrics from journal %s "%(jo["uuid"],))
    metric = {}
    metric["uuid"] = jo["uuid"] #redundant (dev/debug)
    if "scopusMetrics" in jo:
      if verbose>2: print("  >>> metrics from journal %s metrics %s"%(jo["uuid"],jo["scopusMetrics"],))
      for m in metrics:
        for a in jo["scopusMetrics"]:
          for y in range(fromyear, currentyear):
            if a["year"] == y:
              if verbose>2: print("  >>> metrics from journal %s metric %s year %d data %s"%(jo["uuid"],m,y,a,))
              if m in a:
                metric[str(y)+"_"+m] = a[m]
    if verbose>2: print("  >>> metrics from journal %s metric %s"%(jo["uuid"],metric,))
    metricdata[jo["uuid"]] = metric.copy()
  return metricdata

def readjson(file,verbose):
  if verbose: print("Read JSON from '%s'"%(file,))
  jsondata = []
  with open(file, 'rb') as f:
    rawjson = json.load(f)
    if verbose>2: print("%s"%(rawjson,))
    jsondata = rawjson["items"]
      
  return jsondata

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
  cfgsec = "CSV"
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
  journalfile = cfg.get(cfgsec,"journalfile") if cfg.has_option(cfgsec,"journalfile") else None
  outputfile = cfg.get(cfgsec,"outputfile") if cfg.has_option(cfgsec,"outputfile") else None

  keywords = None
  if cfg.has_option(cfgsec,"keywords"):
    keywords = json.loads(cfg.get(cfgsec,"keywords"))

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

  jsondata = readjson(inputfile,verbose)
  journaldata = readjson(journalfile,verbose)

  metricdata = parsemetrics(journaldata,verbose)
  items = parsejson(jsondata,keywords,metricdata,verbose)
  output(outputfile,items,verbose)
  
if __name__ == "__main__":
  main(sys.argv[1:])
