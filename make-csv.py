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
import os, sys, getopt
import csv
import json
import re
import requests
import configparser
from pprint import pprint

def output(outputfile,csvdata,verbose):
  # write to outputfile (always)
  with open(outputfile, 'w', newline='', encoding="UTF-8") as f:
    #writer = csv.writer(f, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL) #, encoding="UTF-8"
    writer = csv.DictWriter(f, fieldnames=csvdata[0], delimiter=';', quotechar='"')
    writer.writeheader()
    for row in csvdata:
      #if verbose: print("CSV (%s) write row: %s"%(outputfile,row,))
      writer.writerow(row)

  if verbose: print("Output written to file '%s'"%(outputfile,))

def readjson(researchfile,organisationfile,personfile,journalfile,verbose):
  with open(organisationfile, 'rb') as f:
    organisation = json.load(f)
  with open(personfile, 'rb') as f:
    person = json.load(f)
  with open(journalfile, 'rb') as f:
    journal = json.load(f)

  #with open(researchfile, encoding='utf-8') as f:
  with open(researchfile, 'rb') as f:
    research = json.load(f)

  if verbose: print("Read research outputs from '%s'"%(researchfile,))

  return (research,organisation,person,journal)

def getkeywordvalue(keyword,row):
  value = None
  if "keywordGroups" in row:
    for k in row["keywordGroups"]:
      if "keywords" in k:
        for w in k["keywords"]:
          if "uri" in w:
            #if "dk/atira/pure/keywords/"+keyword+"/1" in w["uri"]:
            #  value = "1"
            #if "dk/atira/pure/keywords/"+keyword+"/0" in w["uri"]:
            #  value = "0"
            if "dk/atira/pure/keywords/"+keyword+"/" in w["uri"]:
              value = w["uri"].split("/")[-1] # last index
  return value

def tocsv(kota,assessmenttypes,research,organisation,person,journal,locale,verbose):
  fastcache = { # immediate cache for less http calls
    "countries": {}
  }

  csvdata = []
  allcount=0
  resultcount=0
  for row in research["items"]:
    allcount+=1
    if verbose>1: print("%s start %d"%(row["uuid"],allcount,))
    
    # nb! KOTA value could be the initial GET parameter
    # e.g. GET only those rows from Pure that are to be included in result
    # but it might be relevant when data size is fairly large
    # this is now, however, another scripts issue (get-pure.py).
    kotaOK = False
    if not kota or kota == getkeywordvalue("KOTA",row):
      kotaOK = True
    if verbose>1: print("%s KOTA %s"%(row["uuid"],kotaOK,))

    if kotaOK:
      # fetch needed values per one object

      # organisation
      for o in organisation["items"]:
        if o["uuid"] == row["managingOrganisationalUnit"]["uuid"]:
          for oi in o["ids"]:
            if oi["type"] == "Hanken Costcenter ID":
              # if data was fetched without defining one locale all locale values are present
              # with locale argument the behaviour here can be corrected
              # otherwise there will be for example multiple values and the choice between
              # them is not handled.
              if not locale:
                org = oi["value"]
              elif "typeLocale" in oi:
                if oi["typeLocale"] == locale:
                  org = oi["value"].strip()

      # for JulkaisuVuosi
      for r in row["publicationStatuses"]:
        if r["current"] == True:
          year = r["publicationDate"]["year"]

      # for TekijatiedotTeksti and Tekijat
      authorsText = ""
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
          if len(authorsText)>0: authorsText += "; "
          authorsText += r["name"]["lastName"].strip()+", "+r["name"]["firstName"].strip()
          #collect for later use
          units = []
          if "organisationalUnits" in r:
            for u in r["organisationalUnits"]:
              for o in organisation["items"]:
                if o["uuid"] == u["uuid"]:
                  for oi in o["ids"]:
                    if oi["type"] == "Hanken Costcenter ID":
                      if not locale:
                        units.append(oi["value"])
                      elif "typeLocale" in oi:
                        if oi["typeLocale"] == locale:
                          units.append(oi["value"])
          orcid = None
          if "person" in r:
            if "uuid" in r["person"]:
              for p in person["items"]:
                if "orcid" in p:
                  if p["uuid"] == r["person"]["uuid"]:
                    orcid = p["orcid"].strip()
          author = {
            "last": r["name"]["lastName"].strip(),
            "first": r["name"]["firstName"].strip(),
            "units": units,
            "orcid": orcid
          }
          authors.append(author)

      allisbns = []
      if "isbns" in row:
        allisbns += row["isbns"] # list+list
      if "electronicIsbns" in row:
        allisbns += row["electronicIsbns"] # list+list
      # for JufoTunnus and ISSN (not isbn though)
      jufotunnus = None
      issns = []
      country = None
      if "journalAssociation" in row:
        if "journal" in row["journalAssociation"]:
          journaluuid = row["journalAssociation"]["journal"]["uuid"]
          for j in journal["items"]:
            if journaluuid == j["uuid"]:
              if "externalIdSource" in j:
                if "externalId" in j:
                  if j["externalIdSource"] == "jufo":
                    jufotunnus = j["externalId"]
              if "issns" in j:
                for i in j["issns"]:
                  issns.append(i["value"]) #single value to list
              if "country" in j:
                for c in j["country"]:
                  if "uri" in c:
                    country2letter = c["uri"].split("/")[-1]
                    if country2letter in fastcache["countries"]:
                      country = fastcache["countries"][country2letter]
                    else:
                      #REQUESTS
                      requri = 'https://restcountries.eu/rest/v2/alpha'
                      reqparams = {'codes':country2letter}
                      if verbose>1: print("%s call %s %s"%(row["uuid"],requri,country2letter,))
                      try:
                        q = requests.get(requri, params=reqparams)
                        if q.status_code != 200:
                          print("%s Error! HTTP status code: %s for %s %s"%(row["uuid"],str(q.status_code),requri,country2letter,))
                        else:
                          qdata = json.loads(q.content)
                          for d in qdata:
                            if "numericCode" in d:
                              country = d["numericCode"]
                              fastcache["countries"][country2letter] = country
                      except requests.exceptions.RequestException as e:
                        print(e)
                        print(requests)
                        #sys.exit(3)

      # for JulkaisutyyppiKoodi (and deciding do we generate XML)
      assessmenttype = None # reset
      if "assessmentType" in row:
        for r in row["assessmentType"]:
          assessmenttype = r["value"].split()[0]

      # 2. output CSV now that all values are fetched
      # is the publication of right type (nb! list in the settings/configurations file)
      if assessmenttype in assessmenttypes:
        resultcount+=1
        csvitem = {}
        #
        csvitem["JulkaisunOrgTunnus"] = row["uuid"] # JulkaisunOrgTunnus
        csvitem["JulkaisunOrgYksikot"] = org # JulkaisunOrgYksikot (only one)
        csvitem["JulkaisuVuosi"] = str(year) # JulkaisuVuosi
        nimi = row["title"].strip()
        if "subTitle" in row:
          nimi+=": "+row["subTitle"].strip()
        csvitem["JulkaisunNimi"] = nimi # JulkaisunNimi
        csvitem["TekijatiedotTeksti"] = authorsText or "" # TekijatiedotTeksti
        csvitem["TekijoidenLkm"] = str(row["totalNumberOfAuthors"]) if "totalNumberOfAuthors" in row else "" # TekijoidenLkm
        csvitem["SivunumeroTeksti"] = row["pages"].strip() if "pages" in row else "" # SivunumeroTeksti
        csvitem["Artikkelinumero"] = row["articleNumber"] if "articleNumber" in row else "" # Artikkelinumero
        """ may be many
        if len(allisbns)>0:
          isbncount=0 # to-do-ish: must not be greater than 2, which to choose?
          for isbn in allisbns:
            # nb! ISSN and ISBN are mixed in data, separate: ISBN format is loose but at least 10 chars
            if len(isbn)>=10:
              isbncount+=1
              if isbncount <= 2:
                isbn.strip() # ISBN 0 2 ()
        """
        csvitem["JufoTunnus"] = jufotunnus or "" # JufoTunnus
        csvitem["JulkaisumaaKoodi"] = country or "" # JulkaisumaaKoodi
        csvitem["LehdenNimi"] = row["journalAssociation"]["title"]["value"].strip() if "journalAssociation" in row else "" # LehdenNimi
        """ may be many
        if len(allisbns)>0 or len(issns)>0:
          for issn in list(set().union(allisbns, issns)):
            # nb! ISSN and ISBN are mixed in data, separate: ISSN format is strict "NNNN-NNNN"
            if re.search("^[0-9]{4}-[0-9]{3}[0-9xX]$", issn):
              issn.strip() # ISSN 0 2 ()
        """
        csvitem["VolyymiTeksti"] = row["volume"].strip() if "volume" in row else "" # VolyymiTeksti
        csvitem["LehdenNumeroTeksti"] = row["journalNumber"] if "journalNumber" in row else "" # LehdenNumeroTeksti
        csvitem["KonferenssinNimi"] = row["hostPublicationTitle"] if "hostPublicationTitle" in row else "" # KonferenssinNimi
        publisher = None
        if "publisher" in row:
          if "name" in row["publisher"]:
            for a in row["publisher"]["name"]:
              # nb! technically may be many but choose randomly last
              publisher = a["value"]
        csvitem["KustantajanNimi"] = publisher.strip() if publisher else "" # KustantajanNimi
        csvitem["KustannuspaikkaTeksti"] = row["placeOfPublication"].strip() if "placeOfPublication" in row else "" # KustannuspaikkaTeksti
        csvitem["EmojulkaisunNimi"] = row["hostPublicationTitle"].strip() if "hostPublicationTitle" in row else "" # EmojulkaisunNimi
        editorsText = ""
        if "hostPublicationEditors" in row:
          # make a string list
          for e in row["hostPublicationEditors"]:
            if "lastName" in e:
              if "firstName" in e:
                if len(editorsText)>0: editorsText += "; "
                editorsText += e["lastName"].strip()+", "+e["firstName"].strip()
          editorsText # EmojulkaisunToimittajatTeksti
        csvitem["EmojulkaisunToimittajatTeksti"] = editorsText
        csvitem["JulkaisutyyppiKoodi"] = assessmenttype # JulkaisutyyppiKoodi
        tieteenalakoodit = []
        if "keywordGroups" in row:
          for g in row["keywordGroups"]:
            if "keywords" in g:
              for a in g["keywords"]:
                if "uri" in a:
                  if "/dk/atira/pure/core/keywords/" in a["uri"]:
                    if "value" in a:
                      koodi = a["value"]
                      koodi = koodi.split(" ")[0]
                      koodi = koodi.replace(",","") # remove comma "," if it exists, e.g. "612,1"->"6121"
                      if re.search("^[0-9]+$", koodi):
                        if verbose>1: print("%s tieteenalakoodi %s"%(row["uuid"],koodi,))
                        tieteenalakoodit.append(koodi)
        """ may be many
        if len(tieteenalakoodit) > 0:
          jnro = 0
          for x in tieteenalakoodit:
            jnro += 1
            if jnro <= 6:
              # TieteenalaKoodi has attribute so make variable
              str(x) # TieteenalaKoodi
              str(jnro) # JNro
        """
        csvitem["YhteisjulkaisuKVKytkin"] = getkeywordvalue("YhteisjulkaisuKVKytkin",row) # YhteisjulkaisuKVKytkin
        csvitem["JulkaisunKansainvalisyysKytkin"] = getkeywordvalue("JulkaisunKansainvalisyysKytkin",row) # JulkaisunKansainvalisyysKytkin
        language = None
        if "language" in row:
          for a in row["language"]:
            # nb! technically may be many but choose randomly last, then split with "_" i.e. "fi_FI" -> "fi"
            language = a["uri"].split("/")[-1].split("_")[0]
            # nb! there are some odd language values for ex. "/dk/atira/pure/core/languages/italian"?
            if   language=="chinese":        language = "zh"
            elif language=="italian":        language = "it"
            elif language=="polish":         language = "pl"
            elif language=="portuguese":     language = "pt"
            if language == "und": # value 99 is not used for unknown
              language = None
        csvitem["JulkaisunKieliKoodi"] = language # JulkaisunKieliKoodi
        csvitem["AvoinSaatavuusKoodi"] = getkeywordvalue("avoinsaatavuuskoodi",row) # AvoinSaatavuusKoodi
        csvitem["YhteisjulkaisuYritysKytkin"] = getkeywordvalue("YhteisjulkaisuYritysKytkin",row) # YhteisjulkaisuYritysKytkin
        csvitem["RinnakkaistallennettuKytkin"] = getkeywordvalue("rinnakkaistallennettukytkin",row) # RinnakkaistallennettuKytkin
        if getkeywordvalue("rinnakkaistallennettukytkin",row):
          if "1" == getkeywordvalue("rinnakkaistallennettukytkin",row):
            # find "free text" beside rinnakkaistallennettukytkin:
            rinnakkaistallennettutexts = []
            for g in row["keywordGroups"]:
              if "logicalName" in g:
                if "rinnakkaistallennettukytkin" in g["logicalName"]:
                  for k in g["keywords"]:
                    if "parent" in k:
                      if "rinnakkaistallennettukytkin/1" in k["parent"]:
                        rinnakkaistallennettutexts.append(k["value"].strip())
            """ may be many
            if len(rinnakkaistallennettutexts)>0:
              for t in rinnakkaistallennettutexts:
                t # RinnakkaistallennusOsoiteTeksti
            """
        doi = None
        link = None
        if "electronicVersions" in row:
          for a in row["electronicVersions"]:
            # nb! technically may be many but choose randomly last
            if "doi" in a:
              doi  = a["doi"].strip()
              # nb! strip www part from DOI which is apparently added by Pure
              doi = doi.replace("https://doi.org/","")
            if "link" in a:
              link = a["link"].strip()
        csvitem["DOI"] = doi # DOI
        csvitem["PysyvaOsoiteTeksti"] = link # PysyvaOsoiteTeksti
        #duplicate: row["uuid"] # LahdetietokannanTunnus
        """ may be many
        if len(authors)>0:
          for a in authors:
            a["last"] # Sukunimi
            a["first"] # Etunimet
            if len(a["units"]) > 0:
              # may be many (again)
              for u in a["units"]:
                # YksikkoKoodi may have attribute but not used by SHH
                u # YksikkoKoodi
            if a["orcid"]:
              a["orcid"] # ORCID
        #"""
        csvdata.append(csvitem)

  if verbose: print("There were %d objects in total"%(allcount,))
  if verbose: print("Result has %d objects"%(resultcount,))

  return csvdata

def usage():
  print("""usage: make-csv.py [OPTIONS]

OPTIONS
-h, --help          : this message and exit
-K, --kota <kota>   : keyword value to filter data
-L, --locale <loc>  : locale to filter data

Source files with defaults from configuration:
-R, --research <file>
-O, --organisation <file>
-P, --person <file>
-J, --journal <file>

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
  kota = cfg.get(cfgsec,"kota") if cfg.has_option(cfgsec,"kota") else exit("No kota in config. Exit.")
  assessmenttypes = None
  if cfg.has_option(cfgsec,"assessmenttypes"):
    assessmenttypes = json.loads(cfg.get(cfgsec,"assessmenttypes"))
  else:
    exit("Missing mandatory configuration value for assessmenttypes. Exit.")
  researchfile = cfg.get(cfgsec,"researchfile") if cfg.has_option(cfgsec,"researchfile") else None
  organisationfile = cfg.get(cfgsec,"organisationfile") if cfg.has_option(cfgsec,"organisationfile") else None
  personfile = cfg.get(cfgsec,"personfile") if cfg.has_option(cfgsec,"personfile") else None
  journalfile = cfg.get(cfgsec,"journalfile") if cfg.has_option(cfgsec,"journalfile") else None
  outputfile = cfg.get(cfgsec,"outputfile") if cfg.has_option(cfgsec,"outputfile") else None

  # read possible arguments. all optional given that defaults suffice
  try:
    opts, args = getopt.getopt(argv,"hK:L:R:O:P:J:o:vq",["help","kota=","locale=","research=","organisation=","person=","journal=","output=","verbose","quiet"])
  except getopt.GetoptError as err:
    print(err)
    sys.exit(2)
  for opt, arg in opts:
    if opt in ("-h", "--help"):
      usage()
      sys.exit(0)
    elif opt in ("-K", "--kota"): kota = arg
    elif opt in ("-L", "--locale"): locale = arg
    elif opt in ("-R", "--research"): researchfile = arg
    elif opt in ("-O", "--organisation"): organisationfile = arg
    elif opt in ("-P", "--person"): personfile = arg
    elif opt in ("-J", "--journal"): journalfile = arg
    elif opt in ("-o", "--output"): outputfile = arg
    elif opt in ("-v", "--verbose"): verbose += 1
    elif opt in ("-q", "--quiet"): verbose -= 1

  if not researchfile: exit("No researchfile. Exit.")
  if not organisationfile: exit("No organisationfile. Exit.")
  if not personfile: exit("No personfile. Exit.")
  if not journalfile: exit("No journalfile. Exit.")
  if not outputfile: exit("No outputfile. Exit.")

  if verbose: print("Start processing with kota filter: %s"%(kota,))
  if verbose: print("Assessment types filter: (%d) %s"%(len(assessmenttypes),assessmenttypes,))
  (research,organisation,person,journal) = readjson(researchfile,organisationfile,personfile,journalfile,verbose)
  csvdata = tocsv(kota,assessmenttypes,research,organisation,person,journal,locale,verbose)
  output(outputfile,csvdata,verbose)
  
if __name__ == "__main__":
  main(sys.argv[1:])
