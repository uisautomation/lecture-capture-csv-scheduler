#!/usr/bin/env python3
"""
Schedule opencast events from csv input (file)

Usage:
    oc_sched.py (-h|--help)
    oc_sched.py [--input=PATH]

Options:

    -h, --help                      Show a brief usage summary.

    -i, --input=PATH                Path (including filename) of the csv file to ingest
                                    [default: ./lecture-schedule.csv]

Environment Variables:

    OCUSER                          Username of opencast user for event scheduling (default: admin)
    OCPASSWORD                      Password for opencast user for event scheduling (no default)

"""  # noqa:E501

import os
import csv
import sys
import json
import requests
import datetime
import logging
import docopt
from requests.auth import HTTPBasicAuth
from requests_toolbelt import MultipartEncoder

logging.basicConfig(filename='lecture-schedule.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(message)s')

inputfile = 'lecture-schedule.csv'
url = os.environ.get('OCURL', 'https://admin.lecturecapture.uis.cam.ac.uk')
user = os.environ.get('OCUSER', "admin")
password = os.environ.get("OCPASSWORD", 'password')
fieldnames = ("location",
              "title",
              "series",
              "startTime",
              "stopTime",
              "workflow",
              "courseDescription")


def oc_create_event(m):
    """opencast request for event creation"""
    event_url = url + '/api/events'
    try:
        request = requests.post(event_url, data=m,
                                headers={'Content-Type': m.content_type},
                                auth=HTTPBasicAuth(user, password))
    except requests.exceptions.RequestException as e:
        logging.error(e)
        sys.exit(1)
    logging.info('status: ' + str(request.status_code))
    logging.info(request.text)
    return request


def oc_acl(row):
    """Create opencast acl for an event"""
    acl = []
    keys = ["allow", "action", "role"]
    write = [True, "write", "ROLE_ADMIN"]
    read = [True, "read", "ROLE_USER"]
    acl.append(dict(zip(keys, write)))
    acl.append(dict(zip(keys, read)))
    return acl


def oc_metadata(row):
    """Create opencast metadata for an event"""
    t = datetime.datetime.strptime(row["startTime"], "%Y-%m-%dT%H:%M:%SZ")
    meta = []
    innerdict = {}
    innerdict["fields"] = []
    innerdict["flavor"] = "dublincore/episode"
    keys = ["id", "value"]
    title = ["title", row["title"]]
    description = ["description", row["courseDescription"]]
    startDate = ["startDate", t.strftime("%Y-%m-%d")]
    startTime = ["startTime", t.strftime("%H:%M:%SZ")]
    innerdict["fields"].append(dict(zip(keys, title)))
    innerdict["fields"].append(dict(zip(keys, description)))
    innerdict["fields"].append(dict(zip(keys, startDate)))
    innerdict["fields"].append(dict(zip(keys, startTime)))
    meta.append(innerdict)
    return meta


def oc_sched(row):
    """Create opencast schedule for an event"""
    sched = {"agent_id": row["location"],
             "start": row["startTime"],
             "end": row["stopTime"],
             "inputs": ["default"]}
    return sched


def oc_process(row):
    """Create opencast processing details for an event"""
    conf = {"flagForCutting": "false",
            "flagForReview": "false",
            "publishToEngage": "true",
            "publishToHarvesting": "true",
            "straightToPublishing": "true"}
    process = {"workflow": row["workflow"], "configuration": conf}
    return process


def oc_lecture_sched(inputfile):
    """Read in csv file row by row, assemble multipart form fields and create events"""
    with open(inputfile) as csv_file:
        csv_reader = csv.DictReader(csv_file, fieldnames, delimiter=',')
        logging.info('Loaded file: ' + inputfile)
        line_count = 0
        for row in csv_reader:
            # skip the first line as this will be column headings
            if line_count > 0:
                logging.info("procesing row :" + str(line_count))
                m = MultipartEncoder(
                    fields={'acl': json.dumps(oc_acl(row)),
                            'metadata': json.dumps(oc_metadata(row)),
                            'scheduling': json.dumps(oc_sched(row)),
                            'processing': json.dumps(oc_process(row))})
                oc_create_event(m)
            line_count += 1

if __name__ == "__main__":

    opts = docopt.docopt(__doc__, options_first=True)
    if os.environ.get("OCPASSWORD") is None:
        print("No opencast password defined - please set OCPASSWORD environment variable")
        logging.error("No opencast password defined")
        sys.exit(1)
    if opts['--input']:
        oc_lecture_sched(opts['--input'])
    else:
        oc_lecture_sched(inputfile)
