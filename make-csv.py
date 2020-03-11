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
import jufo

# values read from config
keywords = None
metrics = None
metricstartyear = None
metricyears = None

def makerow(verbose):
  #rowheader = columns.copy()
  #rowheader.sort()
  rowheader = [
    "Research output Pure ID",
    "Research output UUID",
    "Research output DOI",
    "Research output additional link",
    "Research output title",
    "Research output abstract",
    "Research output language",
    "Research output type", # nb! second to last part of url
    "Research output subtype",
    "Research output category",
    "Research output assessment type category",
    "Research output assessment type code",
    "Research output assessment type",
    "Research output status year",
    "Research output status",
    "Research output workflow",
    "Research output total number of authors",
    "Research output number of internal authors",
    "Research output number of external authors",
    # person
    "Person role",
    "Person last name",
    "Person first name",
    "Person first last name",
    "Person country",
    "Person organisational units name",
    "Person external organisations name",
    "Person external organisations country",
    "Person Pure ID",
    "Person Personec ID",
    "Person ORCID ID",
    "Person Oodi hlo ID",
    "Person MasterDB ID",
    "Person Student ID",
    "Person UUID",
    "Person external UUID",
    "Person organisational units UUID",
    "Person external organisations UUID",
    # / person
    "Managing organisational unit UUID",
    "Managing organisational unit name",
    "Journal ISSN",
    "Journal title",
    "Journal type",
    "Journal UUID",
    "Journal Pure ID",
    "Journal Workflow",
    "Journal country",
    "Research output volume",
    "Research output journal number",
    "Research output pages",
    "Research output article number",
    "Research output edition",
    "Research output ISBNs",
    "Research output open access permission",
    # keywords
    "Keyword avoinsaatavuuskoodi",
    "Keyword JulkaisunKansainvalisyysKytkin",
    "Keyword KOTA",
    "Keyword rinnakkaistallennettukytkin",
    "Keyword rinnakkaistallennettuosoite",
    "Keyword YhteisjulkaisuKVKytkin",
    "Keyword YhteisjulkaisuYritysKytkin",
    "Keyword AoS_keywords",
    #
    "Keyword field 511",
    "Keyword field 512",
    "Keyword field 513",
    "Keyword field 517",
    "Keyword field 518",
    "Keyword field 112",
    "Keyword field 113",
    # / keywords
    # metrics
    "Scopus metrics citescore 2014",
    "Scopus metrics sjr 2014", #...
    "Scopus metrics snip 2014",
    "Jufo metrics 2014",
    "Scopus metrics citescore 2015", #...
    "Scopus metrics sjr 2015",
    "Scopus metrics snip 2015",
    "Jufo metrics 2015",
    "Scopus metrics citescore 2016",
    "Scopus metrics sjr 2016",
    "Scopus metrics snip 2016",
    "Jufo metrics 2016",
    "Scopus metrics citescore 2017",
    "Scopus metrics sjr 2017",
    "Scopus metrics snip 2017",
    "Jufo metrics 2017",
    "Scopus metrics citescore 2018",
    "Scopus metrics sjr 2018",
    "Scopus metrics snip 2018",
    "Jufo metrics 2018",
    "Scopus metrics citescore 2019",
    "Scopus metrics sjr 2019",
    "Scopus metrics snip 2019",
    "Jufo metrics 2019",
    "Scopus metrics citescore 2020",
    "Scopus metrics sjr 2020",
    "Scopus metrics snip 2020",
    "Jufo metrics 2020",
    "Scopus metrics citescore 2021",
    "Scopus metrics sjr 2021",
    "Scopus metrics snip 2021",
    "Jufo metrics 2021",
    "Scopus metrics citescore 2022",
    "Scopus metrics sjr 2022",
    "Scopus metrics snip 2022",
    "Jufo metrics 2022",
    # / metrics
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
def parsejson(jsondata,metricdata,journaldata,persondata,externalpersondata,externalorganisationdata,verbose):
  global keywords,metrics,metricstartyear,metricyears

  items = []
  for j in jsondata:
    item = {}
    item["Research output Pure ID"] = j["pureId"]
    item["Research output UUID"] = j["uuid"]

    electronicVersions_doi = None
    if "electronicVersions" in j:
      for a in j["electronicVersions"]:
        if "doi" in a:
          electronicVersions_doi = a["doi"]
    item["Research output DOI"] = electronicVersions_doi

    additionalLinks_url = None
    if "additionalLinks" in j:
      for a in j["additionalLinks"]:
        if "url" in a:
          additionalLinks_url = a["url"]
    item["Research output additional link"] = additionalLinks_url

    item["Research output title"] = j["title"]["value"]

    item["Research output abstract"] = js_value("abstract","text",j)

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
    item["Research output language"] = language

    item["Research output type"] = None #jpart("type","uri",j)
    if "type" in j:
      if "uri" in j["type"]:
        item["Research output type"] = j["type"]["uri"].split("/")[-2] # nb! second last part of ".../../THIS/that"
    item["Research output subtype"] = jpart("type","uri",j)
    item["Research output category"] = js_value("term","text",j["category"]) #jpart("category","uri",j)
    item["Research output assessment type category"] = None
    item["Research output assessment type code"] = jpart("assessmentType","uri",j)
    item["Research output assessment type"] = None
    if item["Research output assessment type code"]:
      item["Research output assessment type category"] = item["Research output assessment type code"][0]
    if "assessmentType" in j:
      item["Research output assessment type"] = js_value("term","text",j["assessmentType"])

    publicationStatuses_publicationDate_year = None
    publicationStatuses_publicationStatus = None
    if "publicationStatuses" in j:
      for a in j["publicationStatuses"]:
        publicationStatuses_publicationDate_year = a["publicationDate"]["year"]
        publicationStatuses_publicationStatus = jpart("publicationStatus","uri",a)
    item["Research output status year"] = publicationStatuses_publicationDate_year
    item["Research output status"] = publicationStatuses_publicationStatus

    item["Research output workflow"] = jpart("workflow","workflowStep",j)
    item["Research output total number of authors"] = str(jv("totalNumberOfAuthors",j))

    # nb! next would be personAssociation, but data addition done last, see below

    managingOrganisationalUnit_uuid = None
    managingOrganisationalUnit_name = None
    if "managingOrganisationalUnit" in j:
      managingOrganisationalUnit_uuid = j["managingOrganisationalUnit"]["uuid"]
      managingOrganisationalUnit_name = js_value("name","text",j["managingOrganisationalUnit"])
    item["Managing organisational unit UUID"] = managingOrganisationalUnit_uuid
    item["Managing organisational unit name"] = managingOrganisationalUnit_name

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
    item["Journal ISSN"] = journalAssociation_issn
    item["Journal title"] = journalAssociation_title
    item["Journal type"] = journalAssociation_journal_type
    item["Journal UUID"] = journal_uuid
    # fetch from journaldata
    item["Journal Pure ID"] = ""
    item["Journal Workflow"] = ""
    item["Journal country"] = ""
    for a in journaldata:
      if a["uuid"] == journal_uuid:
        item["Journal Pure ID"] = a["pureId"]
        item["Journal Workflow"] = jpart("workflow","workflowStep",a)
        if "country" in a:
          item["Journal country"] = js_value("term","text",a["country"])

    item["Research output volume"] = jv("volume",j)
    item["Research output journal number"] = jv("journalNumber",j)
    item["Research output pages"] = jv("pages",j)
    item["Research output article number"] = jv("articleNumber",j)
    item["Research output edition"] = jv("edition",j)

    item["Research output ISBNs"] = "" # nb! different from others!
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
            item["Research output ISBNs"] += ","
          item["Research output ISBNs"] += isbn.strip()

    item["Research output open access permission"] = jpart("openAccessPermission","uri",j)

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
                    keyword_value = js_value("term","text",s)
              # nb! rinnakkaistallennettuosoite is some what different
              if "rinnakkaistallennettukytkin" == k and keyword_value:
                if "1" in keyword_value:
                  # find "free text" beside rinnakkaistallennettukytkin:
                  if "freeKeywords" in c:
                    for f in c["freeKeywords"]: # list
                      if "freeKeywords" in f: # another one
                        for it in f["freeKeywords"]: # list of strings
                          item["Keyword rinnakkaistallennettuosoite"] = it.strip() # last one
      # this will ensure a value for each row even if None
      item["Keyword "+k] = keyword_value
      if not "Keyword rinnakkaistallennettuosoite" in item:
        item["Keyword rinnakkaistallennettuosoite"] = None # nb! different keyword, ensure column existence

    # core keywords (tieteenalakoodit)
    for t in ["511","512","513","517","518","112","113"]:
      item["Keyword field "+t] = None
      if "keywordGroups" in j:
        for g in j["keywordGroups"]:
          if "keywordContainers" in g:
            for c in g["keywordContainers"]:
              if "structuredKeyword" in c:
                s = c["structuredKeyword"]
                if "uri" in s:
                  if "/dk/atira/pure/core/keywords/" in s["uri"]:
                    svalue = js_value("term","text",s)
                    code_check = svalue
                    code_check = code_check.split(" ")[0]
                    code_check = code_check.replace(",","") # remove comma "," if it exists, e.g. "612,1"->"6121"
                    if re.search("^"+t+"$", code_check):
                      if verbose>2: print("%s tieteenalakoodi %s"%(j["uuid"],code_check,))
                      item["Keyword field "+t] = svalue

    # get scopusMetrics from journals
    for m in metrics:
      for y in range(metricstartyear, metricstartyear+metricyears):
        if "jufo" == m:
          mkey = "Jufo metrics "+str(y)
        else:
          mkey = "Scopus metrics "+m+" "+str(y)
        # this will make sure column exists in every row
        item[mkey] = None
        # get the actual value if exists
        if journal_uuid in metricdata:
          if mkey in metricdata[journal_uuid]:
            item[mkey] = metricdata[journal_uuid][mkey]

    # nb! row multiplying data
    # so do this/these last

    # multiply rows per person!
    added_persons = False # keep track if nothing was added
    # calculate numberOfInternalAuthors first so it copies fully for every row
    item["Research output number of internal authors"] = jv("totalNumberOfAuthors",j)
    item["Research output number of external authors"] = 0
    if "personAssociations" in j:
      for a in j["personAssociations"]:
        roleIsOK = False # test for role
        role = jpart("personRole","uri",a)
        # nb! authors and editors
        if role == "author" or role == "editor":
          roleIsOK = True
        if roleIsOK:
          if "externalPerson" in a:
            item["Research output number of internal authors"] -= 1
            if item["Research output number of internal authors"] < 0:
              item["Research output number of internal authors"] = 0
            item["Research output number of external authors"] += 1

    if "personAssociations" in j:
      for a in j["personAssociations"]:
        added_persons = True # .. will be added
        # reset values here
        personAssociations_personRole = None
        personAssociations_name_firstName = None
        personAssociations_name_lastName = None
        personAssociations_person_name = None
        personAssociations_country = ""
        personAssociations_organisationalUnits_name = None
        personAssociations_externalOrganisations_name = None
        personAssociations_externalOrganisations_country = None
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
          if "country" in a:
            personAssociations_country = js_value("term","text",a["country"]) #jpart("country","uri",a)
          # fetch person id values, internal and external
          if "person" in a:
            personAssociations_person_uuid = a["person"]["uuid"]
            if "name" in a["person"]:
              personAssociations_person_name = js_value("name","text",a["person"])
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
              for o in externalorganisationdata:
                if o["uuid"] == b["uuid"]:
                  if "address" in o:
                    if "country" in o["address"]:
                      personAssociations_externalOrganisations_country = js_value("term","text",o["address"]["country"])
          # add person values to item here, overwrite if 1+ round
          item["Person role"] = personAssociations_personRole
          item["Person first name"] = personAssociations_name_firstName
          item["Person last name"] = personAssociations_name_lastName
          item["Person first last name"] = personAssociations_person_name
          item["Person country"] = personAssociations_country
          item["Person organisational units name"] = personAssociations_organisationalUnits_name
          item["Person external organisations name"] = personAssociations_externalOrganisations_name
          item["Person external organisations country"] = personAssociations_externalOrganisations_country
          item["Person Pure ID"] = personAssociations_person_pureid
          item["Person Personec ID"] = personAssociations_person_employeeid
          item["Person ORCID ID"] = personAssociations_person_orcid
          item["Person Oodi hlo ID"] = personAssociations_person_oodiid
          item["Person MasterDB ID"] = personAssociations_person_masterdbid
          item["Person Student ID"] = personAssociations_person_studentid
          item["Person UUID"] = personAssociations_person_uuid
          item["Person external UUID"] = personAssociations_externalPerson_uuid
          item["Person organisational units UUID"] = personAssociations_organisationalUnits_uuid
          item["Person external organisations UUID"] = personAssociations_externalOrganisations_uuid

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
  global metrics,metricstartyear,metricyears

  if verbose>1: print("Parse metrics from year %d to %d"%(metricstartyear,metricstartyear+metricyears,))

  metricdata = {}
  for jo in journaldata:
    if verbose>2: print("  >>> metrics from journal %s "%(jo["uuid"],))
    metric = {}
    metric["uuid"] = jo["uuid"] #redundant (dev/debug)
    for y in range(metricstartyear, metricstartyear+metricyears):
      for m in metrics:
        # jufo resides elsewhere
        if m == "jufo":
          if "externalIdSource" in jo and "externalId" in jo:
            if "jufo" == jo["externalIdSource"]:
              jufoid = jo["externalId"]
              jufojson = jufo.get(jufoid)
              # store if for later use
              jufocfg = configparser.ConfigParser()
              jufocfgsec = "LOCAL"
              jufocfg.read('Jufo.cfg')
              if not jufocfg.has_section(jufocfgsec):
                print("Failed reading config. Exit")
                exit(1)
              # continue w/ [cfgsec] config
              jufodatadir = jufocfg.get(jufocfgsec,"datadir") if jufocfg.has_option(jufocfgsec,"datadir") else "."
              jufodatadir += "/"
              with open(jufodatadir+"jufo_%s.json"%(jufoid,), "w") as f:
                json.dump(jufojson, f)
              for ju in jufojson: # should have only one
                if "Jufo_ID" in ju and "Jufo_%d"%(y,) in ju:
                  if jufoid == ju["Jufo_ID"]:
                    metric["Jufo metrics "+str(y)] = ju["Jufo_%d"%(y,)]
        else:
          if "scopusMetrics" in jo:
            if verbose>2: print("  >>> metrics from journal %s metrics %s"%(jo["uuid"],jo["scopusMetrics"],))
            for a in jo["scopusMetrics"]:
              if a["year"] == y:
                if verbose>2: print("  >>> metrics from journal %s metric %s year %d data %s"%(jo["uuid"],m,y,a,))
                if m in a:
                  metric["Scopus metrics "+m+" "+str(y)] = a[m]
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
-o, --externalorganisation <file>

Output file with default from configuration:
-O, --output <file>

-v, --verbose       : increase verbosity
-q, --quiet         : reduce verbosity
""")

def main(argv):
  global keywords,metrics,metricstartyear,metricyears

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
  externalorganisationfile = cfg.get(cfgsec,"externalorganisationfile") if cfg.has_option(cfgsec,"externalorganisationfile") else None
  outputfile = cfg.get(cfgsec,"outputfile") if cfg.has_option(cfgsec,"outputfile") else None

  if cfg.has_option(cfgsec,"keywords"):
    keywords = json.loads(cfg.get(cfgsec,"keywords"))
  if cfg.has_option(cfgsec,"metrics"):
    metrics = json.loads(cfg.get(cfgsec,"metrics"))
  if cfg.has_option(cfgsec,"metricstartyear"):
    metricstartyear = int(cfg.get(cfgsec,"metricstartyear"))
  if cfg.has_option(cfgsec,"metricyears"):
    metricyears = int(cfg.get(cfgsec,"metricyears"))

  # read possible arguments. all optional given that defaults suffice
  try:
    opts, args = getopt.getopt(argv,"hr:j:p:e:o:O:vq",["help","research=","journal=","person=","externalperson=","externalorganisation=","output=","verbose","quiet"])
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
    elif opt in ("-e", "--externalperson"): externalpersonfile = arg
    elif opt in ("-o", "--externalorganisation"): externalorganisationfile = arg
    elif opt in ("-O", "--output"): outputfile = arg
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
  externalorganisationdata = readjson(externalorganisationfile,verbose)

  metricdata = parsemetrics(journaldata,verbose)
  items = parsejson(jsondata,metricdata,journaldata,persondata,externalpersondata,externalorganisationdata,verbose)
  output(outputfile,items,verbose)
  
if __name__ == "__main__":
  main(sys.argv[1:])
