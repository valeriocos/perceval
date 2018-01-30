# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2017 Bitergia
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
#     Santiago Dueñas <sduenas@bitergia.com>
#

import logging
import hashlib
import requests

from grimoirelab.toolkit.datetime import datetime_to_utc, str_to_datetime, datetime_utcnow
from grimoirelab.toolkit.uris import urijoin

from ...backend import (Backend,
                        BackendCommand,
                        BackendCommandArgumentParser)
from ...client import HttpClient, RateLimitHandler
from ...utils import DEFAULT_DATETIME


MARVEL_URL = "https://developer.marvel.com/"
MARVEL_API_URL = "http://gateway.marvel.com/v1/public/"

# sleep time and retries to deal with connection/server problems
SLEEP_TIME = 1
MAX_RETRIES = 5
ITEMS_PER_PAGE = 100

logger = logging.getLogger(__name__)


class Marvel(Backend):
    """Marvel backend for Perceval.

    This class allows the fetch the comics provided by Marvel
    repository.

    :param public_key : the public key
    :param private_key: the private key
    :param tag: label used to mark the data
    :param cache: collect comics already retrieved in cache
    :param archive: an archive to read/store data fetched by the backend
    :param sleep_for_rate: sleep until rate limit is reset
    :param max_retries: number of max retries to a data source
        before raising a RetryError exception
    :param sleep_time: time to sleep in case
        of connection problems
    :param items_per_page: number of comics fetched per page
    """
    version = '0.1.0'

    def __init__(self, public_key, private_key,
                 tag=None, cache=None, archive=None,
                 sleep_for_rate=False,
                 max_retries=MAX_RETRIES, sleep_time=SLEEP_TIME, items_per_page=ITEMS_PER_PAGE):
        super().__init__(MARVEL_URL, tag=tag, cache=cache, archive=archive)

        self.public_key = public_key
        self.private_key = private_key

        self.sleep_for_rate = sleep_for_rate
        self.max_retries = max_retries
        self.sleep_time = sleep_time
        self.items_per_page = items_per_page

        self.client = None

    def fetch(self, from_date=DEFAULT_DATETIME):
        """Fetch the comics from the Marvel repository.

        The method retrieves the comics
        modified since the given date.

        :param from_date: obtain comics updated since this date

        :returns: a generator of comics
        """
        if not from_date:
            from_date = DEFAULT_DATETIME

        from_date = datetime_to_utc(from_date)

        kwargs = {"from_date": from_date}
        items = super().fetch("comic", **kwargs)

        return items

    def fetch_items(self, **kwargs):
        """Fetch the comics"""

        from_date = kwargs['from_date']

        comic_groups = self.client.comics(from_date=from_date)

        for comics in comic_groups:
            for comic in comics:

                if Marvel.is_comic_data_available('characters', comic):
                    characters = self.client.comic_data(comic['characters']['collectionURI'])
                    comic['characters_data'] = characters
                if Marvel.is_comic_data_available('creators', comic):
                    creators = self.client.comic_data(comic['creators']['collectionURI'])
                    comic['creators_data'] = creators
                if Marvel.is_comic_data_available('stories', comic):
                    stories = self.client.comic_data(comic['stories']['collectionURI'])
                    comic['stories_data'] = stories

                yield comic

    @classmethod
    def has_caching(cls):
        """Returns whether it supports caching items on the fetch process.

        :returns: this backend supports items cache
        """
        return False

    @classmethod
    def has_archiving(cls):
        """Returns whether it supports archiving items on the fetch process.

        :returns: this backend supports items archive
        """
        return True

    @classmethod
    def has_resuming(cls):
        """Returns whether it supports to resume the fetch process.

        :returns: this backend supports items resuming
        """
        return True

    @staticmethod
    def metadata_id(item):
        """Extracts the identifier from a Marvel item."""

        return str(item['id'])

    @staticmethod
    def metadata_updated_on(item):
        """Extracts the update time from a Marvel item.

        The timestamp used is extracted from 'modified' field.
        This date is converted to UNIX timestamp format. As Marvel
        dates are in UTC the conversion is straightforward.

        :param item: item generated by the backend

        :returns: a UNIX timestamp
        """
        ts = item['modified']
        ts = str_to_datetime(ts)

        return ts.timestamp()

    @staticmethod
    def metadata_category(item):
        """Extracts the category from a Marvel item.

        This backend only generates one type of item which is
        'issue'.
        """
        return 'comic'

    @staticmethod
    def is_comic_data_available(attribute, comic_info):
        available = True

        if attribute not in comic_info:
            available = False
        elif comic_info[attribute]['available'] == 0:
            available = False

        return available

    def _init_client(self, from_archive=False):
        """Init client"""

        return MarvelClient(self.public_key, self.private_key,
                            self.sleep_for_rate,
                            self.max_retries, self.sleep_time,
                            self.items_per_page,
                            self.archive, from_archive)


class MarvelClient(HttpClient, RateLimitHandler):
    """Client for retieving information from Marvel API"""

    _characters = {} # TODO internal users cache
    _creators = {}  # TODO internal users orgs cache

    def __init__(self, public_key, private_key,
                 sleep_for_rate=False,
                 sleep_time=SLEEP_TIME, max_retries=MAX_RETRIES,
                 items_per_page=ITEMS_PER_PAGE,
                 archive=None, from_archive=False):
        self.public_key = public_key
        self.private_key = private_key
        self.items_per_page = items_per_page

        super().__init__(MARVEL_API_URL, sleep_time=sleep_time, max_retries=max_retries,
                         extra_status_forcelist=[429],
                         archive=archive, from_archive=from_archive)
        super().setup_rate_limit_handler(sleep_for_rate=sleep_for_rate)

    def sign_request(self, payload):
        """Sign request when fetching the data from the API

        :param payload: the payload of the request where to appen the signature
        """
        timestamp = str(datetime_utcnow().timestamp())
        md5 = hashlib.md5((timestamp + self.private_key + self.public_key)
                          .encode('utf-8')).hexdigest()

        sign = {'ts': timestamp, 'hash': md5, 'apikey': self.public_key}

        return {**payload, **sign}

    def comics(self, from_date=None):
        """Fetch comics from the API

        :param from_date: collect comics modified after a given date
        """
        payload = {
            'offset': 0,
            'orderBy': 'modified',
            'limit': self.items_per_page
        }

        if from_date:
            payload['modifiedSince'] = from_date.isoformat()

        payload = self.sign_request(payload)

        path = urijoin(MARVEL_API_URL, "comics")
        return self.fetch_items(path, payload)

    def comic_data(self, path):
        """Fetch related data of the comic from the API"""

        payload = {
            'offset': 0,
            'limit': self.items_per_page
        }

        payload = self.sign_request(payload)

        return [item for group in self.fetch_items(path, payload) for item in group]

    def fetch_items(self, path, payload):
        """Return the items from Marvel API using pagination"""

        logger.debug("Get Marvel paginated items from " + path)

        response = self.fetch(path, payload=payload)
        items_info = response.json()['data']

        total = items_info['total']
        count = items_info['count']

        while True:
            yield items_info['results']

            if count == total:
                break

            next_offset = count

            payload.update({'offset': next_offset})
            payload = self.sign_request(payload)

            response = self.fetch(path, payload=payload)

            items_info = response.json()['data']
            count = count + items_info['count']

            logger.debug("Fetched: %i/%i" % (count, total))

    def _fetch_from_archive(self, url, payload, headers):
        payload.pop('ts', None)
        payload.pop('hash', None)
        payload.pop('apikey', None)

        response = self.archive.retrieve(url, payload, headers)

        if not isinstance(response, requests.Response):
            raise response

        return response

    def _fetch_from_remote(self, url, payload, headers, method, stream, verify):
        if method == self.GET:
            response = self.session.get(url, params=payload, headers=headers, stream=stream, verify=verify)
        else:
            response = self.session.post(url, data=payload, headers=headers, stream=stream, verify=verify)

        try:
            response.raise_for_status()
        except Exception as e:
            response = e
            raise e
        finally:
            if self.archive:
                payload.pop('ts', None)
                payload.pop('hash', None)
                payload.pop('apikey', None)

                self.archive.store(url, payload, headers, response)

        return response


class MarvelCommand(BackendCommand):
    """Class to run Marvel backend from the command line."""

    BACKEND = Marvel

    @staticmethod
    def setup_cmd_parser():
        """Returns the Marvel argument parser."""

        parser = BackendCommandArgumentParser(from_date=True,
                                              token_auth=False,
                                              cache=False)

        # Marvel options
        group = parser.parser.add_argument_group('Marvel arguments')
        group.add_argument('--public-key', dest='public_key',
                           help="public key used to sign requests")
        group.add_argument('--private-key', dest='private_key',
                           help="private key used to sign requests")
        group.add_argument('--items-per-page', dest='items_per_page',
                           default=ITEMS_PER_PAGE, type=int,
                           help="items fetched per page")
        group.add_argument('--sleep-for-rate', dest='sleep_for_rate',
                           action='store_true',
                           help="sleep for getting more rate")

        # Generic client options
        group.add_argument('--max-retries', dest='max_retries',
                           default=MAX_RETRIES, type=int,
                           help="number of API call retries")
        group.add_argument('--sleep-time', dest='sleep_time',
                           default=SLEEP_TIME, type=int,
                           help="sleeping time between API call retries")

        return parser
