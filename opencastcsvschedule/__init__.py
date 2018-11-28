"""
Schedule opencast events from csv input (file)

Usage:
    oc_sched.py (-h|--help)
    oc_sched.py --base-url=URL [--user=USER] [--password-file=PATH] [--quiet]
        [--input=PATH]

Options:

    -h, --help                      Show a brief usage summary.
    -q, --quiet                     Decrease verbosity of logging.

    --base-url=URL                  URL of opencast instance.

    --user=USER                     Username of Opencast user [default: admin]
    --password-file=PATH            Path to file containing Opencast user password.
                                    (Leading/trailing whitespace is stripped.)

    -i, --input=PATH                Path (including filename) of the csv file to ingest. If
                                    omitted, standard input is used.

"""
import csv
import json
import logging
import urllib.parse
import sys

import docopt
from dateutil.parser import parse
import pytz
import requests
from requests.auth import HTTPBasicAuth
from requests_toolbelt import MultipartEncoder


# Expected header for input CSV.
EXPECTED_CSV_HEADER = [
    "location", "title", "series", "startTime", "stopTime", "workflow", "courseDescription",
    "vleUri", "sequenceUri", "sequenceIndex"
]


class ProcessingError(RuntimeError):
    pass


def main():
    # Parse command line options
    opts = docopt.docopt(__doc__, options_first=True)

    # Configure logging
    logging.basicConfig(
        level=logging.WARN if opts['--quiet'] else logging.INFO,
        format='%(asctime)s %(message)s'
    )

    # Load password
    with open(opts['--password-file']) as fobj:
        oc_password = fobj.read().strip()

    # Form arguments to schedule_events()
    schedule_event_args = {
        'base_url': opts['--base-url'],
        'user': opts['--user'],
        'password': oc_password,
    }

    # Attempt to schedule events catching processing errors.
    try:
        if opts['--input'] is not None:
            with open(opts['--input']) as fobj:
                schedule_events(input_fobj=fobj, **schedule_event_args)
        else:
            schedule_events(input_fobj=sys.stdin, **schedule_event_args)
    except ProcessingError:
        # Log error and exit with error status
        logging.error('Aborting processing due to error')
        sys.exit(1)

    # Signal normal exit
    sys.exit(0)


def schedule_events(input_fobj, base_url, user, password):
    """
    Read events from CSV and schedule them in Opencast.

    """
    # Read header from CSV and check that it conforms to our expectation.
    header = next(csv.reader(input_fobj))
    if header[:len(EXPECTED_CSV_HEADER)] != EXPECTED_CSV_HEADER:
        logging.error('Bad header in csv file')
        logging.error('Header was: %s', ','.join(header))
        logging.error('Expected: %s', ','.join(EXPECTED_CSV_HEADER))
        raise ProcessingError()

    logging.info('Loading CSV')
    csv_reader = csv.DictReader(input_fobj, EXPECTED_CSV_HEADER)

    # Form the events API URL from the base URL.
    events_api_url = urllib.parse.urljoin(base_url, 'api/events')

    # For each input row, form each of the mulitpart form fields required by the opencast API.
    for row in csv_reader:
        logging.info(
            'Scheduling event "%s" at %s', row['title'], _parse_date(row['startTime']).isoformat()
        )

        # Create multipart form encoding for event
        body_data = MultipartEncoder(fields={
            'acl': json.dumps(oc_acl()),
            'metadata': json.dumps(oc_metadata(row)),
            'scheduling': json.dumps(oc_sched(row)),
            'processing': json.dumps(oc_process(row)),
        })

        # Attempt to schedule it
        try:
            response = requests.post(
                events_api_url, data=body_data, headers={'Content-Type': body_data.content_type},
                auth=HTTPBasicAuth(user, password)
            )
            response.raise_for_status()
        except Exception as e:
            logging.error('Error posting event')
            logging.error('Row was: %r', row)
            logging.exception(e)


def oc_acl():
    return [
        {'role': 'ROLE_ADMIN', 'action': 'write', 'allow': True},
        {'role': 'ROLE_USER', 'action': 'read', 'allow': True},
    ]


def oc_metadata(row):
    """Create opencast metadata for an event"""
    t = _parse_date(row['startTime'])

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
    sched = {
        "agent_id": row["location"],
        "start": row["startTime"],
        "end": row["stopTime"],
        "inputs": ["camera", "screen", "AudioSource"],
    }
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


def _parse_date(s):
    """
    Parse date from a date string as defined in the CSV.

    """
    return parse(s).astimezone(pytz.utc)


if __name__ == "__main__":
    main()
