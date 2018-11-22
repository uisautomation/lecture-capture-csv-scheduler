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
    OCURL                           URL for the opencast instance

"""  # noqa:E501

import os
import csv
import sys
import json
import requests
import datetime
import logging
import docopt
import pytz
from requests.auth import HTTPBasicAuth
from requests_toolbelt import MultipartEncoder
from dateutil.parser import parse

logging.basicConfig(filename='lecture-schedule.log',
                    level=logging.DEBUG,
                    format='%(asctime)s %(message)s')

inputfile = 'lecture-schedule.csv'
url = os.environ.get('OCURL', 'https://admin.lecturecapture.uis.cam.ac.uk')
user = os.environ.get('OCUSER', "admin")
password = os.environ.get("OCPASSWORD", 'password')
fieldnames = ["location",
              "title",
              "series",
              "startTime",
              "stopTime",
              "workflow",
              "courseDescription",
              "vleUri",
              "sequenceUri",
              "sequenceIndex"]


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
    logging.info("status: %s" % str(request.status_code))
    logging.info(request.text)
    return request


def oc_acl():
    return [
        {'role': 'ROLE_ADMIN', 'action': 'write', 'allow': True},
        {'role': 'ROLE_USER', 'action': 'read', 'allow': True},
    ]


def oc_metadata(row):
    """Create opencast metadata for an event"""
    t = parse(row['startTime']).astimezone(pytz.utc)
    def _make_field(id_, value):
        return {'id': id_, 'value': value}

    return [
        {
            'flavor': 'dublincore/episode',
            'fields': [
                _make_field('title', row['title']),
                _make_field('description', row['courseDescription']),
                _make_field('startDate', t.strftime("%Y-%m-%d")),
                _make_field('startTime', t.strftime("%H:%M:%SZ")),
            ],
        }
    ]


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
        header = next(csv.reader(csv_file))
        if header[:len(fieldnames)] != fieldnames:
            logging.error("Bad header in csv file: %s" %  inputfile )
            logging.error(header)
            sys.exit(1)
        csv_reader = csv.DictReader(csv_file, fieldnames)
        logging.info("Loaded file: %s" % inputfile)
        for row in csv_reader:
            m = MultipartEncoder(
                fields={'acl': json.dumps(oc_acl()),
                        'metadata': json.dumps(oc_metadata(row)),
                        'scheduling': json.dumps(oc_sched(row)),
                        'processing': json.dumps(oc_process(row))})
            oc_create_event(m)

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
