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

def makerow(verbose):
  #rowheader = columns.copy()
  #rowheader.sort()
  rowheader = [
    "Pure ID",
    "Publication UUID",
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
    "Publication Workflow",
    "totalNumberOfAuthors",
    "numberOfInternalAuthors",
    "numberOfExternalAuthors",
    # person
    "personAssociations_personRole",
    "personAssociations_name_lastName",
    "personAssociations_name_firstName",
    "personAssociations_country",
    "personAssociations_organisationalUnits_name",
    "personAssociations_externalOrganisations_name",
    "Person Pure ID",
    "Employee Personec ID",
    "ORCID ID",
    "Oodi hlo ID",
    "MasterDB ID",
    "Student ID",
    "personAssociations_person_uuid",
    "personAssociations_externalPerson_uuid",
    "personAssociations_organisationalUnits_uuid",
    "personAssociations_externalOrganisations_uuid",
    # / person
    "managingOrganisationalUnit_uuid",
    "managingOrganisationalUnit_name",
    "journalAssociation_issn",
    "journalAssociation_title",
    "journalAssociation_journal_type",
    "Journal UUID",
    "Journal Pure ID",
    "Journal Workflow",
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
    #
    ,"keyword_field511"
    ,"keyword_field512"
    ,"keyword_field513"
    ,"keyword_field517"
    ,"keyword_field518"
    ,"keyword_field112"
    ,"keyword_field113"
    #
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
  #columns = [ x for row in items for x in row.keys() ]
  #columns = list(set(columns))
  columns = makerow(verbose)

  # write to outputfile (always)
  with open(outputfile, 'w', newline='', encoding="UTF-8") as f:
    writer = csv.DictWriter(f, fieldnames=columns, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL, extrasaction='ignore')
    writer.writeheader()
    count=0
    for row in items:
      count+=1
      if verbose>2: print("Output CSV (%s) with row: %s"%(outputfile,row,))
      writer.writerow(row)

  if verbose: print("Output written to file '%s' with %d columns and %d rows"%(outputfile,len(columns),count,))

# Helper functions for repeatedly used part of code
# get direct value from json with name
def jv(objectname,jsonitem):
  value = None
  if objectname in jsonitem:
    value = jsonitem[objectname]
  return value
# get "value" under a list in a subelement of an object (list is for locales and we rely on one locale)
def js_value(objectname,subname,jsonitem):
  value = None
  if objectname in jsonitem:
    if subname in jsonitem[objectname]:
      for a in jsonitem[objectname][subname]: # on a rare occasion there may be many, return last
        if "value" in a:
          value = a["value"]
  return value
# get last part of objects subelements text value where value is separated by slash (/)
def jpart(objectname,subname,jsonitem):
  lastpart = None
  if objectname in jsonitem:
    if subname in jsonitem[objectname]:
      lastpart = jsonitem[objectname][subname].split("/")[-1] # last part of ".../../THIS"
  return lastpart

# go thru given JSON. Look for bits were interested in and write to output file (CSV)
def parsejson(jsondata,keywords,metricdata,journaldata,persondata,externalpersondata,verbose):
  global currentyear

  items = []
  for j in jsondata:
    item = {}
    item["Pure ID"] = j["pureId"]
    item["Publication UUID"] = j["uuid"]

    electronicVersions_doi = None
    if "electronicVersions" in j:
      for a in j["electronicVersions"]:
        if "doi" in a:
          electronicVersions_doi = a["doi"]
    item["electronicVersions_doi"] = electronicVersions_doi

    item["title"] = j["title"]["value"]

    item["abstract"] = js_value("abstract","text",j)

    language = None
    if "language" in j:
      # nb! last part but then split with "_" i.e. "fi_FI" -> "fi"
      language = jpart("language","uri",j).split("_")[0]
      # nb! there are some odd language values for ex. "/dk/atira/pure/core/languages/italian"?
      if   language=="chinese":        language = "zh"
      elif language=="italian":        language = "it"
      elif language=="polish":         language = "pl"
      elif language=="portuguese":     language = "pt"
      if language == "und": # value 99 is not used for unknown
        language = None
    item["language"] = language

    item["type"] = jpart("type","uri",j)
    item["category"] = jpart("category","uri",j)
    item["assessmentType"] = jpart("assessmentType","uri",j)

    publicationStatuses_publicationDate_year = None
    publicationStatuses_current = None
    publicationStatuses_publicationStatus = None
    if "publicationStatuses" in j:
      for a in j["publicationStatuses"]:
        publicationStatuses_publicationDate_year = a["publicationDate"]["year"]
        publicationStatuses_current = a["current"]
        publicationStatuses_publicationStatus = jpart("publicationStatus","uri",a)
    item["publicationStatuses_publicationDate_year"] = publicationStatuses_publicationDate_year
    item["publicationStatuses_current"] = publicationStatuses_current
    item["publicationStatuses_publicationStatus"] = publicationStatuses_publicationStatus

    item["Publication Workflow"] = jpart("workflow","workflowStep",j)
    item["totalNumberOfAuthors"] = str(jv("totalNumberOfAuthors",j))

    # nb! next would be personAssociation, but data addition done last, see below

    managingOrganisationalUnit_uuid = None
    managingOrganisationalUnit_name = None
    if "managingOrganisationalUnit" in j:
      managingOrganisationalUnit_uuid = j["managingOrganisationalUnit"]["uuid"]
      managingOrganisationalUnit_name = js_value("name","text",j["managingOrganisationalUnit"])
    item["managingOrganisationalUnit_uuid"] = managingOrganisationalUnit_uuid
    item["managingOrganisationalUnit_name"] = managingOrganisationalUnit_name

    journalAssociation_issn = None
    journalAssociation_title = None
    journalAssociation_journal_type = None
    # for scopus metrics fetching (also):
    journal_uuid = None
    if "journalAssociation" in j:
      a = j["journalAssociation"]
      if "issn" in a:
        journalAssociation_issn = a["issn"]["value"]
      if "title" in a:
        journalAssociation_title = a["title"]["value"]
      if "journal" in a:
        if "type" in a["journal"]:
          journalAssociation_journal_type = js_value("term","text",a["journal"]["type"])
        if "uuid" in a["journal"]:
          journal_uuid = a["journal"]["uuid"]
    item["journalAssociation_issn"] = journalAssociation_issn
    item["journalAssociation_title"] = journalAssociation_title
    item["journalAssociation_journal_type"] = journalAssociation_journal_type
    item["Journal UUID"] = journal_uuid
    # fetch from journaldata
    item["Journal Pure ID"] = ""
    item["Journal Workflow"] = ""
    for a in journaldata:
      if a["uuid"] == journal_uuid:
        item["Journal Pure ID"] = a["pureId"]
        item["Journal Workflow"] = jpart("workflow","workflowStep",a)

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

    item["openAccessPermission"] = jpart("openAccessPermission","uri",j)

    # keywords pivot
    # - to title: keywordGroups.keywords.value => compromise this with setting value since there is no guarantee a keyword exists for all research-outputs
    # - to value: keywordGroups.type.value
    for k in keywords:
      keyword_value = None
      if "keywordGroups" in j:
        for g in j["keywordGroups"]:
          if "keywordContainers" in g:
            for c in g["keywordContainers"]:
              if "structuredKeyword" in c:
                s = c["structuredKeyword"]
                if "uri" in s:
                  if "dk/atira/pure/keywords/"+k+"/" in s["uri"]:
                    keyword_value = s["uri"].split("/")[-1] # last index
      item["keyword_"+k] = keyword_value

    # core keywords (tieteenalakoodit)
    for t in ["511","512","513","517","518","112","113"]:
      item["keyword_field"+t] = None
      if "keywordGroups" in j:
        for g in j["keywordGroups"]:
          if "keywordContainers" in g:
            for c in g["keywordContainers"]:
              if "structuredKeyword" in c:
                s = c["structuredKeyword"]
                if "uri" in s:
                  if "/dk/atira/pure/core/keywords/" in s["uri"]:
                    if "term" in s:
                      if "text" in s["term"]:
                        for w in s["term"]["text"]:
                          if "value" in w:
                            code_check = w["value"]
                            code_check = code_check.split(" ")[0]
                            code_check = code_check.replace(",","") # remove comma "," if it exists, e.g. "612,1"->"6121"
                            if re.search("^"+t+"$", code_check):
                              if verbose>2: print("%s tieteenalakoodi %s"%(j["uuid"],code_check,))
                              item["keyword_field"+t] = w["value"]

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
    
    # nb! row multiplying data
    # so do this/these last

    # multiply rows per person!
    added_persons = False # keep track if nothing was added
    # calculate numberOfInternalAuthors first so it copies fully for every row
    item["numberOfInternalAuthors"] = jv("totalNumberOfAuthors",j)
    item["numberOfExternalAuthors"] = 0
    if "personAssociations" in j:
      for a in j["personAssociations"]:
        roleIsOK = False # test for role
        role = jpart("personRole","uri",a)
        # nb! authors and editors
        if role == "author" or role == "editor":
          roleIsOK = True
        if roleIsOK:
          if "externalPerson" in a:
            item["numberOfInternalAuthors"] -= 1
            if item["numberOfInternalAuthors"] < 0:
              item["numberOfInternalAuthors"] = 0
            item["numberOfExternalAuthors"] += 1

    if "personAssociations" in j:
      for a in j["personAssociations"]:
        added_persons = True # .. will be added
        # reset values here
        personAssociations_personRole = None
        personAssociations_name_firstName = None
        personAssociations_name_lastName = None
        personAssociations_country = ""
        personAssociations_organisationalUnits_name = None
        personAssociations_externalOrganisations_name = None
        personAssociations_person_pureid = ""
        personAssociations_person_employeeid = ""
        personAssociations_person_orcid = ""
        personAssociations_person_oodiid = ""
        personAssociations_person_masterdbid = ""
        personAssociations_person_studentid = ""
        personAssociations_person_uuid = ""
        personAssociations_externalPerson_uuid = ""
        personAssociations_organisationalUnits_uuid = None
        personAssociations_externalOrganisations_uuid = None

        roleIsOK = False # test for role
        role = jpart("personRole","uri",a)
        # nb! authors and editors
        if role == "author" or role == "editor":
          roleIsOK = True
        if roleIsOK:
          personAssociations_personRole = role
          if "name" in a:
            personAssociations_name_firstName = a["name"]["firstName"]
            personAssociations_name_lastName = a["name"]["lastName"]
          personAssociations_country = jpart("country","uri",a)
          # fetch person id values, internal and external
          if "person" in a:
            personAssociations_person_uuid = a["person"]["uuid"]
            for p in persondata:
              if p["uuid"] == personAssociations_person_uuid:
                personAssociations_person_pureid = p["pureId"]
                if "orcid" in p: personAssociations_person_orcid = p["orcid"]
                # list of ids
                if "ids" in p:
                  for i in p["ids"]:
                    if "type" in i:
                      if "uri" in i["type"]:
                        if i["type"]["uri"] == "/dk/atira/pure/person/personsources/employee":
                          personAssociations_person_employeeid = jpart("value","value",i)
                        if i["type"]["uri"] == "/dk/atira/pure/person/personsources/oodi":
                          personAssociations_person_oodiid = jpart("value","value",i)
                        if i["type"]["uri"] == "/dk/atira/pure/person/personsources/masterdb":
                          personAssociations_person_masterdbid = jpart("value","value",i)
                        if i["type"]["uri"] == "/dk/atira/pure/person/personsources/studentid":
                          personAssociations_person_studentid = jpart("value","value",i)
          if "externalPerson" in a:
            personAssociations_externalPerson_uuid = a["externalPerson"]["uuid"]
            for p in externalpersondata:
              if p["uuid"] == personAssociations_externalPerson_uuid:
                # should not replace but same field yes
                personAssociations_person_pureid = p["pureId"]
                # nb! external persons do not have internal ids
          # person organisational data
          if "organisationalUnits" in a:
            for b in a["organisationalUnits"]:
              personAssociations_organisationalUnits_uuid = b["uuid"]
              personAssociations_organisationalUnits_name = js_value("name","text",b)
          if "externalOrganisations" in a:
            for b in a["externalOrganisations"]:
              personAssociations_externalOrganisations_uuid = b["uuid"]
              personAssociations_externalOrganisations_name = js_value("name","text",b)
          # add person values to item here, overwrite if 1+ round
          item["personAssociations_personRole"] = personAssociations_personRole
          item["personAssociations_name_firstName"] = personAssociations_name_firstName
          item["personAssociations_name_lastName"] = personAssociations_name_lastName
          item["personAssociations_country"] = personAssociations_country
          item["personAssociations_organisationalUnits_name"] = personAssociations_organisationalUnits_name
          item["personAssociations_externalOrganisations_name"] = personAssociations_externalOrganisations_name
          item["Person Pure ID"] = personAssociations_person_pureid
          item["Employee Personec ID"] = personAssociations_person_employeeid
          item["ORCID ID"] = personAssociations_person_orcid
          item["Oodi hlo ID"] = personAssociations_person_oodiid
          item["MasterDB ID"] = personAssociations_person_masterdbid
          item["Student ID"] = personAssociations_person_studentid
          item["personAssociations_person_uuid"] = personAssociations_person_uuid
          item["personAssociations_externalPerson_uuid"] = personAssociations_externalPerson_uuid
          item["personAssociations_organisationalUnits_uuid"] = personAssociations_organisationalUnits_uuid
          item["personAssociations_externalOrganisations_uuid"] = personAssociations_externalOrganisations_uuid
          # and append here (not at "root" loop end)
          items.append(item.copy()) #nb! make a copy (not reference)
        #/roleIsOK
      #/
    #/personAssociations
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

Source files with defaults from configuration:
-r, --research <file>
-j, --journal <file>
-p, --person <file>
-e, --externalperson <file>

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
  verbose = 1 # default minor messages
  researchfile = cfg.get(cfgsec,"researchfile") if cfg.has_option(cfgsec,"researchfile") else None
  journalfile = cfg.get(cfgsec,"journalfile") if cfg.has_option(cfgsec,"journalfile") else None
  personfile = cfg.get(cfgsec,"personfile") if cfg.has_option(cfgsec,"personfile") else None
  externalpersonfile = cfg.get(cfgsec,"externalpersonfile") if cfg.has_option(cfgsec,"externalpersonfile") else None
  outputfile = cfg.get(cfgsec,"outputfile") if cfg.has_option(cfgsec,"outputfile") else None

  keywords = None
  if cfg.has_option(cfgsec,"keywords"):
    keywords = json.loads(cfg.get(cfgsec,"keywords"))

  # read possible arguments. all optional given that defaults suffice
  try:
    opts, args = getopt.getopt(argv,"hr:j:p:e:o:vq",["help","research=","journal=","person=","externalperson=","output=","verbose","quiet"])
  except getopt.GetoptError as err:
    print(err)
    sys.exit(2)
  for opt, arg in opts:
    if opt in ("-h", "--help"):
      usage()
      sys.exit(0)
    elif opt in ("-r", "--research"): researchfile = arg
    elif opt in ("-j", "--journal"): journalfile = arg
    elif opt in ("-p", "--person"): personfile = arg
    elif opt in ("-e", "--externalperson"): personfile = arg
    elif opt in ("-o", "--output"): outputfile = arg
    elif opt in ("-v", "--verbose"): verbose += 1
    elif opt in ("-q", "--quiet"): verbose -= 1

  if not researchfile: exit("No research file. Exit.")
  if not journalfile: exit("No journal file. Exit.")
  if not personfile: exit("No person file. Exit.")
  if not externalpersonfile: exit("No externalperson file. Exit.")
  if not outputfile: exit("No output file. Exit.")

  jsondata = readjson(researchfile,verbose)
  journaldata = readjson(journalfile,verbose)
  persondata = readjson(personfile,verbose)
  externalpersondata = readjson(externalpersonfile,verbose)

  metricdata = parsemetrics(journaldata,verbose)
  items = parsejson(jsondata,keywords,metricdata,journaldata,persondata,externalpersondata,verbose)
  output(outputfile,items,verbose)
  
if __name__ == "__main__":
  main(sys.argv[1:])
