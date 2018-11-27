# Schedule Opencast Events from CSV

This repository contains a small utility to schedule Opencast events from an
input CSV.

**This utility requires at least version 1.1 of the Opencast API. This version
was first released in Opencast 6.**

## Installation

```console
$ pip install git+https://github.com/uisautomation/lecture-capture-csv-scheduler
$ opencast_csv_schedule --help  # print usage summary
```

## Usage

```console
$ echo "super-secret-password" > opencast.password
$ opencast_csv_schedule \
    --input=schedule.csv --base-url=https://opencast.invalid/ \
    --user=some-opencast-user --password-file=opencast.password
```

## Development

When developing this script, it is useful to combine a virtualenv with pip's
concept of an "editable" install which uses symlinks instead of copies when
installing:

```console
$ git clone git@github.com:uisautomation/lecture-capture-csv-scheduler.git
$ cd lecture-capture-csv-scheduler
$ python3 -m virtualenv ./venv
$ source ./venv/bin/activate
$ pip install -e .
```
