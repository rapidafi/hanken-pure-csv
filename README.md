# HANKEN (HARIS): PURE -> CSV

Read Pure data via API as JSON and produce CSV files.

## TABLE OF CONTENTS

1. [SETUP](#setup)
    1. [Requirements](#requirements)
    2. [Install](#install)
        1. [Development](#development)
        2. [Production](#production)
    3. [Configuration](#configuration)

2. [RUN](#run)

3. [DOCUMENTATION](#documentation)
    1. [Get data](#get-data)
        1. [Note about \<api\> and \<output\>](#note-about-api-and-output)
        2. [Additional information](#additional-information)
    2. [Produce CSV](#produce-csv)

## SETUP

### Requirements

* CentOS 7 (not actual requirement but this was the target environment during development)
* Python 3 (versions 3.6.6 and 3.7.3 were used during development)
    * See [requirements](requirements) for Python modules required
    * Recommended module virtualenv

### Install

#### Development

```shell
sudo yum install python36 python36-pip python36-devel python36-virtualenv
# in directory where you want your virtualenv directory. maybe home directory.
python -m venv [my virtual environment name]
source [my virtual environment name]/bin/activate
# cd (change directory) to where you have source code
# for example
cd hanken-pure-csv/
pip install -r requirements
```

#### Production

```shell
sudo yum install python36 python36-pip
# cd (change directory) to where you have source code
# for example
cd hanken-pure-csv/
sudo pip install -r requirements
```

### Configuration

Copy or rename [Pure-example.cfg](Pure-example.cfg) to `Pure.cfg`. Change all TODO values to meaningful values.

Section [API] must have values for _hostname_, _uri_, _apikey_, _username_ and _password_ which are all used to access Pure API.


## RUN

TL;DR

```shell
python get-pure.py research-outputs
python get-pure.py journals
# when source files are present:
python make-csv.py
```

## DOCUMENTATION

### Get data

Data from Pure can be loaded via APIs. More in-depth specifications about these APIs should be read from Pure API documentation itself as these scripts only read data from a few APIs with straight-forward strategy: all data. The APIs provide very useful means for this and details can be seen in script [get-pure.py](get-pure.py).

The script [get-pure.py](get-pure.py) has following usage:

`python get-pure.py [OPTIONS] <API>`

where:

`<API>`

* name of the API to be called
* mandatory

OPTIONS

-h or --help

* shows short usage of script and exits

-H or --host `<hostname>`

* hostname of Pure API
* defaults to configuration value then to $PURE\_HOSTNAME
* recommended to use configuration value

-u or --uri `<uri>`

* base part of URI (up until last slash) of Pure API
* defaults to configuration value then to $PURE\_URI
* recommended to use configuration value

-L or --locale `<locale>`

* locale to filter data
* default locale is _en\_GB_

-s or --size `<size>`

* number of elements to read from Pure API at once
* also the max number of elements in optional splitted files 
* defaults to 1000 (API default is 10)

-o or --output `<output>`

* filename(s) to save the loaded result to
* may be many files if given argument -S|--split
* default is "`<API>`.json"

-S or --split

* causes the `<output>` files to be split with max `<size>` entries each file
* splitted files are created with pattern like "api.json" => "api-0001.json", e.g. catenate "-" and four digits with running number after "api" and postfix with ".json"

-v or --verbose

* increase console output

-q or --quiet

* reduce console output


#### Additional information

The following Pure API data is considered mandatory. These are also named in configuration template (example):

* research-outputs
* journals

The following data from Pure API was looked at but a decision was made not to include (no added value to result):

* organisational-units
* persons


### Produce CSV

Once all mandatory JSON files are present a call to script called [make-csv.py](make-csv.py) can be made. The script loads research-outputs and journals into memory and produces a CSV file.

The columns chosen for result has been reduced in iterations. With no limitations there were over 300 columns and after collaborating iterations the column count has reduced to 64. There's still plenty to work around, ay.

Command line arguments, all optional, for [make-csv.py](make-csv.py) are:

TODO

-h or --help

* shows short usage of script and exits

-L or --locale `<locale>`

* locale to filter data
* needed only if JSON data has locales (normally shouldn't)

-r or --research `<researchfile>`

* name of the JSON file with Pure research-outputs data
* defaults to configuration value

-j or --journal `<journalfile>`

* name of the JSON file with Pure journal data
* defaults to configuration value

-o or --output `<outputfile>`

* name of the file where result is written
* file will be overwritten if it exists
* defaults to configuration value

-v or --verbose

* increase console output

-q or --quiet

* reduce console output

