#!/usr/bin/env python3
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

import json
import datetime
import os
import re
import sys
import unittest
import httpretty
import pkg_resources

# Hack to make sure that tests import the right packages
# due to setuptools behaviour
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
pkg_resources.declare_namespace('perceval.backends')

from grimoirelab.toolkit.datetime import datetime_to_utc, str_to_datetime
from perceval.backend import BackendCommandArgumentParser
from perceval.utils import DEFAULT_DATETIME
from perceval.backends.core.marvel import (MARVEL_URL, SLEEP_TIME,
                                           MAX_RETRIES, ITEMS_PER_PAGE,
                                           Marvel,
                                           MarvelCommand,
                                           MarvelClient)
from tests.base import TestCaseBackendArchive

MARVEL_API_URL = "http://gateway.marvel.com/v1/public/"


def read_file(filename, mode='r'):
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), filename), mode) as f:
        content = f.read()
    return content


def setup_server(from_date=None):
    DIR = 'data/marvel'

    for f in os.listdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), DIR)):
        if f.startswith("empty"):
            continue

        info = f.split(".")
        offset = info[-1]
        path = MARVEL_API_URL + '/'.join([i for i in info[:-1]])
        content = read_file(DIR + '/' + f)

        add_uri = True
        if re.match("^comics\.\d+$", f) and from_date:
            json_content = json.loads(content)
            modified_date = json_content['data']['results'][0]['modified']
            add_uri = str_to_datetime(modified_date).replace(tzinfo=None) >= from_date

            if add_uri:
                content = content.replace('"offset": 26', '"offset": 0')
                content = content.replace('"offset": 28', '"offset": 2')
                content = content.replace('"offset": 30', '"offset": 4')
                content = content.replace('"total": 30', '"total": 6')

        if add_uri:
            httpretty.register_uri(httpretty.GET,
                                   path,
                                   body=content,
                                   params={'offset': offset},
                                   status=200)


class TestMarvelBackend(unittest.TestCase):
    """ GitHub backend tests """

    # def test_fetch_live(self):
    #     public_key = "29b45171402ee95949ff88ab3cdd0e9e"
    #     private_key = "ec10863efa1198b3da023c94514fb470b2b9d26c"
    #
    #     public_key = "42f4da0312cb438a92225a0e714a1902"
    #     private_key = "782a2f0d25ace22ae7a284a0569a0a542e3a4d4a"
    #
    #     marvel = Marvel(public_key, private_key)
    #
    #     from_date = datetime.datetime(2010, 3, 1)
    #     for comic in marvel.fetch(from_date=from_date):
    #         print(json.dumps(comic, sort_keys=True, indent=4))
    #
    # def test_enrich_elasticsearch(self):
    #
    #     import elasticsearch
    #
    #     es = elasticsearch.Elasticsearch(['http://localhost:9200/'])
    #     es.indices.create('comics')
    #
    #     public_key = "29b45171402ee95949ff88ab3cdd0e9e"
    #     private_key = "ec10863efa1198b3da023c94514fb470b2b9d26c"
    #     marvel = Marvel(public_key, private_key)
    #
    #     num = 0
    #     for comic in marvel.fetch(from_date=None):
    #         es.index(index='comics', doc_type='summary', body=comic)
    #
    #         num += 1
    #         if num == 1000:
    #             break

    @httpretty.activate
    def test_initialization(self):
        """Test whether attributes are initializated"""

        marvel = Marvel("public_key", "private_key", tag='test')
        self.assertEqual(marvel.public_key, "public_key")
        self.assertEqual(marvel.private_key, "private_key")
        self.assertEqual(marvel.tag, 'test')
        self.assertEqual(marvel.items_per_page, ITEMS_PER_PAGE)
        self.assertEqual(marvel.sleep_time, SLEEP_TIME)
        self.assertEqual(marvel.sleep_for_rate, False)
        self.assertEqual(marvel.origin, MARVEL_URL)

        # When tag is empty or None it will be set to
        # the value in origin
        marvel = Marvel("public_key", "private_key", items_per_page=2)
        self.assertEqual(marvel.public_key, "public_key")
        self.assertEqual(marvel.private_key, "private_key")
        self.assertEqual(marvel.items_per_page, 2)
        self.assertEqual(marvel.tag, MARVEL_URL)
        self.assertEqual(marvel.origin, MARVEL_URL)

        marvel = Marvel("public_key", "private_key", tag='')
        self.assertEqual(marvel.public_key, "public_key")
        self.assertEqual(marvel.private_key, "private_key")
        self.assertEqual(marvel.tag, MARVEL_URL)
        self.assertEqual(marvel.origin, MARVEL_URL)

    def test_has_caching(self):
        """Test if it returns False when has_caching is called"""

        self.assertEqual(Marvel.has_caching(), False)

    def test_has_resuming(self):
        """Test if it returns True when has_resuming is called"""

        self.assertEqual(Marvel.has_resuming(), True)

    def test_has_archiving(self):
        """Test if it returns True when has_resuming is called"""

        self.assertEqual(Marvel.has_archiving(), True)

    @httpretty.activate
    def test_fetch(self):
        """Test whether a list of issues is returned"""

        setup_server()

        backend = Marvel("public_key_xxx", "private_key_yyy", items_per_page=2)
        comics = [comic for comic in backend.fetch(from_date=None)]

        self.assertEqual(len(comics), 30)

        for c in comics:
            comic = c['data']

            if Marvel.is_comic_data_available('characters', comic):
                self.assertEqual(comic['characters']['available'], len(comic['characters_data']))
            if Marvel.is_comic_data_available('creators', comic):
                self.assertEqual(comic['creators']['available'], len(comic['creators_data']))
            if Marvel.is_comic_data_available('stories', comic):
                self.assertEqual(comic['stories']['available'], len(comic['stories_data']))

    @httpretty.activate
    def test_fetch_from_date(self):
        """ Test when return from date """

        from_date = datetime.datetime(2010, 8, 13)
        setup_server(from_date=from_date)

        backend = Marvel("public_key_xxx", "private_key_yyy", items_per_page=2)
        comics = [comic for comic in backend.fetch(from_date=from_date)]

        self.assertEqual(len(comics), 6)

        for c in comics:
            comic = c['data']

            if Marvel.is_comic_data_available('characters', comic):
                self.assertEqual(comic['characters']['available'], len(comic['characters_data']))
            if Marvel.is_comic_data_available('creators', comic):
                self.assertEqual(comic['creators']['available'], len(comic['creators_data']))
            if Marvel.is_comic_data_available('stories', comic):
                self.assertEqual(comic['stories']['available'], len(comic['stories_data']))

    @httpretty.activate
    def test_fetch_empty(self):
        """ Test when return empty """

        empty_comics = read_file('data/marvel/empty_comics')
        httpretty.register_uri(httpretty.GET, MARVEL_API_URL + 'comics',
                               body=empty_comics,
                               status=200)

        backend = Marvel("public_key_xxx", "private_key_yyy", items_per_page=2)
        comics = [comic for comic in backend.fetch(from_date=None)]

        self.assertEqual(len(comics), 0)


class TestMarvelBackendArchive(TestCaseBackendArchive):
    """Marvel backend tests using the archive"""

    def setUp(self):
        super().setUp()
        self.backend = Marvel("public_key_xxx", "private_key_yyy",
                              archive=self.archive, items_per_page=2)

    @httpretty.activate
    def test_fetch_from_archive(self):
        """Test whether a list of comics is returned from archive"""

        setup_server()
        self._test_fetch_from_archive(from_date=None)

    @httpretty.activate
    def test_fetch_from_date_from_archive(self):
        """Test whether a list of comics is returned from archive after a given date"""

        from_date = datetime.datetime(2010, 8, 13)
        setup_server(from_date=from_date)
        self._test_fetch_from_archive(from_date=from_date)

    @httpretty.activate
    def test_fetch_from_empty_archive(self):
        """Test whether no comics are returned when the archive is empty"""

        empty_comics = read_file('data/marvel/empty_comics')
        httpretty.register_uri(httpretty.GET, MARVEL_API_URL + 'comics',
                               body=empty_comics,
                               status=200)

        self._test_fetch_from_archive(from_date=None)


class TestMarvelClient(unittest.TestCase):
    """Marvel API client tests"""

    def test_init(self):
        """Test whether the client is properly initialized"""

        client = MarvelClient("public_key", "private_key")

        self.assertEqual(client.public_key, 'public_key')
        self.assertEqual(client.private_key, 'private_key')
        self.assertEqual(client.max_retries, MAX_RETRIES)
        self.assertEqual(client.sleep_time, SLEEP_TIME)
        self.assertEqual(client.items_per_page, ITEMS_PER_PAGE)

    @httpretty.activate
    def test_comics(self):
        """Test comics API call"""

        setup_server()
        client = MarvelClient("public_key", "private_key", items_per_page=2)

        pages = [c for c in client.comics()]
        self.assertEqual(len(pages), 15)

    @httpretty.activate
    def test_comics_from_date(self):
        """Test comics from date API call"""

        from_date = datetime.datetime(2010, 8, 13)
        setup_server(from_date=from_date)
        client = MarvelClient("public_key", "private_key", items_per_page=2)

        pages = [c for c in client.comics(from_date=from_date)]
        self.assertEqual(len(pages), 3)

    @httpretty.activate
    def test_comic_data(self):
        """Test comic_data from date API call"""

        setup_server()
        client = MarvelClient("public_key", "private_key", items_per_page=2)

        pages = [c for c in client.comic_data(MARVEL_API_URL + 'comics/34508/characters')]
        self.assertEqual(len(pages), 1)

    @httpretty.activate
    def test_fetch_items(self):
        """Test fetch_items API call"""

        setup_server()
        client = MarvelClient("public_key", "private_key", items_per_page=2)

        items = [item for item in
                 client.fetch_items(MARVEL_API_URL + 'comics/33487/creators', {'offset': 0})]

        self.assertEqual(len(items), 2)

    @httpretty.activate
    def test_empty_fetch_items(self):
        """Test when fetch_items method return an empty result"""

        empty_comics = read_file('data/marvel/empty_comics')
        httpretty.register_uri(httpretty.GET, MARVEL_API_URL + 'comics',
                               body=empty_comics,
                               status=200)

        client = MarvelClient("public_key", "private_key", items_per_page=2)
        items = [item for item in
                 client.fetch_items(MARVEL_API_URL + 'comics', {'offset': 0})]

        self.assertEqual(len(items[0]), 0)


class TestMarvelCommand(unittest.TestCase):
    """MarvelCommand unit tests"""

    def test_backend_class(self):
        """Test if the backend class is GitHub"""

        self.assertIs(MarvelCommand.BACKEND, Marvel)

    def test_setup_cmd_parser(self):
        """Test if it parser object is correctly initialized"""

        parser = MarvelCommand.setup_cmd_parser()
        self.assertIsInstance(parser, BackendCommandArgumentParser)

        args = ['--sleep-for-rate',
                '--max-retries', '5',
                '--sleep-time', '10',
                '--items-per-page', '20',
                '--tag', 'test',
                '--private-key', 'xxx',
                '--public-key', 'yyy',
                '--from-date', '1970-01-01']

        parsed_args = parser.parse(*args)
        self.assertEqual(parsed_args.private_key, 'xxx')
        self.assertEqual(parsed_args.public_key, 'yyy')
        self.assertEqual(parsed_args.sleep_for_rate, True)
        self.assertEqual(parsed_args.max_retries, 5)
        self.assertEqual(parsed_args.sleep_time, 10)
        self.assertEqual(parsed_args.items_per_page, 20)
        self.assertEqual(parsed_args.tag, 'test')
        self.assertEqual(parsed_args.from_date, DEFAULT_DATETIME)


if __name__ == "__main__":
    unittest.main(warnings='ignore')
