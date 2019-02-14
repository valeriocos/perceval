# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2018 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, 51 Franklin Street, Fifth Floor, Boston, MA 02110-1335, USA.
#
# Authors:
#     Valerio Cosentino <valcos@bitergia.com>
#

import datetime
import dateutil.tz
import json
import logging
import time

import googleapiclient.discovery
from google.oauth2 import service_account

from grimoirelab_toolkit.datetime import (datetime_to_utc,
                                          datetime_utcnow,
                                          datetime_to_str,
                                          unixtime_to_datetime)

from ...backend import (Backend,
                        BackendCommand,
                        BackendCommandArgumentParser,
                        uuid)
from ...utils import DEFAULT_DATETIME, DEFAULT_LAST_DATETIME

GITHUB_DEFAULT_DATETIME = datetime.datetime(2011, 2, 12, 0, 0, 0, tzinfo=dateutil.tz.tzutc())
GITHUB_DEFAULT_LAST_DATETIME = datetime_utcnow()

SYNC_TIME = 2
GHARCHIVE_URL = 'https://www.gharchive.org/'

SCOPES = [
        "https://www.googleapis.com/auth/bigquery"
    ]

CATEGORY_EVENTS = "events"

logger = logging.getLogger(__name__)


class BigQueryClient:

    def __init__(self, conf_path):
        self.conf_path = conf_path

        with open(self.conf_path) as f:
            content = f.read()
        self.conf = json.loads(content)

        credentials = service_account.Credentials.from_service_account_file(conf_path, scopes=SCOPES)
        bigquery_service = googleapiclient.discovery.build('bigquery', 'v2', credentials=credentials)
        self.jobs = bigquery_service.jobs()

    def fetch_rows(self, query_reply, project_id):
        current_row = 0

        if int(query_reply['totalRows']) == 0:
            yield {}

        while ("rows" in query_reply) and current_row < int(query_reply['totalRows']):
            for row in query_reply["rows"]:
                transformed = self.transform_row(row, query_reply["schema"]["fields"])
                yield transformed

            current_row += len(query_reply['rows'])

            query_result_request = {
                'projectId': project_id,
                'jobId': query_reply['jobReference']['jobId'],
                'startIndex': current_row
            }

            query_reply = self.jobs.getQueryResults(**query_result_request).execute()

    @staticmethod
    def transform_row(row, fields):
        column_index = 0
        row_data = {}

        for cell in row["f"]:
            field = fields[column_index]
            cell_value = cell['v']

            if cell_value is None:
                pass
            # Otherwise just cast the value
            elif field['type'] == 'INTEGER':
                cell_value = int(cell_value)
            elif field['type'] == 'FLOAT':
                cell_value = float(cell_value)
            elif field['type'] == 'BOOLEAN':
                cell_value = cell_value.lower() == "true"
            elif field['type'] == 'TIMESTAMP':
                datetime_value = unixtime_to_datetime(float(cell_value))
                cell_value = datetime_to_str(datetime_value, '%Y-%m-%d %H:%M:%S')

            row_data[field["name"]] = cell_value
            column_index += 1

        return row_data

    @staticmethod
    def prepare_job_data(query):

        job_data = {
            "configuration": {
                "query": {
                    "query": query,
                }
            }
        }

        job_data['configuration']['query']['useLegacySql'] = False

        return job_data


class GHArchive(Backend):
    """GHArchive backend for Perceval.

    This class allows to fetch GHArchive data via the Google BigQuery service.

    """
    version = '0.1.0'

    CATEGORIES = [CATEGORY_EVENTS]

    def __init__(self, owner, repository, conf_path, tag=None, archive=None):
        super().__init__(GHARCHIVE_URL, tag=tag, archive=archive)

        self.owner = owner
        self.repository = repository
        self.conf_path = conf_path

    def fetch(self, category=CATEGORY_EVENTS, from_date=DEFAULT_DATETIME, to_date=DEFAULT_LAST_DATETIME):
        """Fetch the events of a GitHub repository per day

        The method retrieves the events of a GitHub repository via GHArchive. Optionally,
        it can collect the events from and up to two given dates.

        :param category: the category of items to fetch
        :param from_date: obtain items created since a given date
        :param to_date: obtain items created until a given date (included)

        :returns: a generator of items
        """
        if not from_date or from_date == DEFAULT_DATETIME:
            from_date = GITHUB_DEFAULT_DATETIME
        if not to_date or to_date == DEFAULT_LAST_DATETIME:
            to_date = GITHUB_DEFAULT_LAST_DATETIME

        from_date = datetime_to_utc(from_date)
        to_date = datetime_to_utc(to_date)

        kwargs = {
            'from_date': from_date,
            'to_date': to_date
        }
        items = super().fetch(category, **kwargs)

        return items

    def fetch_items(self, category, **kwargs):
        """Fetch the events per day

        :param category: the category of items to fetch
        :param kwargs: backend arguments

        :returns: a generator of items
        """
        from_date = kwargs['from_date']
        to_date = kwargs['to_date']

        items = self.__fetch_events(from_date, to_date)
        return items

    @classmethod
    def has_archiving(cls):
        """Returns whether it supports archiving items on the fetch process.

        :returns: this backend supports items archive
        """
        return False

    @classmethod
    def has_resuming(cls):
        """Returns whether it supports to resume the fetch process.

        :returns: this backend supports items resuming
        """
        return True

    @staticmethod
    def metadata_id(item):
        """Extracts the identifier from a GHArchive item."""

        return str(item['id'])

    @staticmethod
    def metadata_updated_on(item):
        """Extracts the update time from a GHArchive item.

        The timestamp is based on the current time when the hit was extracted.
        This field is not part of the data provided by GHArchive. It is added
        by this backend.

        :param item: item generated by the backend

        :returns: a UNIX timestamp
        """
        return item['fetched_on']

    @staticmethod
    def metadata_category(item):
        """Extracts the category from a GHArchive item.

        This backend only generates one type of item which is
        'events'.
        """
        return CATEGORY_EVENTS

    def _init_client(self, from_archive=False):
        """Init client"""

        return GHArchiveClient(self.conf_path, self.owner, self.repository)

    def __fetch_events(self, from_date, to_date):
        """Fetch the events of a GitHub repository"""

        daily_events = self.client.events(from_date=from_date, to_date=to_date)

        for event in daily_events:
            fetched_on = datetime_utcnow().timestamp()
            event['fetched_on'] = fetched_on

            id_args = [event['type'], str(fetched_on)]
            event['id'] = uuid(*id_args)

            yield event


class GHArchiveClient(BigQueryClient):

    def __init__(self, conf_path, owner, repository):
        super().__init__(conf_path)
        self.owner = owner
        self.repository = repository
        self.repo_name = self.owner + '/' + self.repository

    def events(self, from_date, to_date):
        current_day = from_date.replace(hour=0, minute=0, second=0, microsecond=0)
        to_day = to_date.replace(hour=0, minute=0, second=0, microsecond=0)

        while current_day < to_day:
            current_day_text = datetime_to_str(current_day, '%Y%m%d')
            logger.info("Querying day %s", current_day_text)

            query = "SELECT type, actor.login, created_at " \
                    "FROM `githubarchive.day.%s` " \
                    "WHERE repo.name = '%s' AND " \
                    "type IN ('WatchEvent', 'ForkEvent', 'DownloadEvent');" % (current_day_text, self.repo_name)

            job_data = self.prepare_job_data(query)
            project_id = self.conf['project_id']

            insert_response = self.jobs.insert(projectId=project_id, body=job_data).execute()

            time.sleep(SYNC_TIME)

            query_reply = self.jobs.getQueryResults(projectId=project_id,
                                                    jobId=insert_response['jobReference']['jobId'],
                                                    startIndex=0).execute()

            for row in self.fetch_rows(query_reply, project_id):
                if not row:
                    logger.info("No results found for day %s", current_day_text)
                    continue

                yield row

            current_day += datetime.timedelta(days=1)


class GHArchiveCommand(BackendCommand):
    """Class to run GHArchive backend from the command line."""

    BACKEND = GHArchive

    @staticmethod
    def setup_cmd_parser():
        """Returns the GHArchive argument parser."""

        parser = BackendCommandArgumentParser(from_date=True,
                                              to_date=True)

        # Positional arguments
        parser.parser.add_argument('owner',
                                   help="GitHub owner")
        parser.parser.add_argument('repository',
                                   help="GitHub repository")
        parser.parser.add_argument('conf_path',
                                   help="Path of the configuration file for Google Big Query service")

        return parser
