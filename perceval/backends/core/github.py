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
#     Alvaro del Castillo San Felix <acs@bitergia.com>
#     Santiago Dueñas <sduenas@bitergia.com>
#     Alberto Martín <alberto.martin@bitergia.com>
#

import json
import logging
import time
import urllib.parse

import requests

from grimoirelab.toolkit.datetime import datetime_to_utc, str_to_datetime
from grimoirelab.toolkit.uris import urijoin

from ...backend import (Backend,
                        BackendCommand,
                        BackendCommandArgumentParser,
                        metadata)
from ...errors import CacheError, RateLimitError
from ...utils import DEFAULT_DATETIME


GITHUB_URL = "https://github.com/"
GITHUB_API_URL = "https://api.github.com"

# Range before sleeping until rate limit reset
MIN_RATE_LIMIT = 10
MAX_RATE_LIMIT = 500

TARGET_ISSUE_FIELDS = ['user', 'assignee', 'assignees', 'comments', 'reactions']

logger = logging.getLogger(__name__)


class GitHub(Backend):
    """GitHub backend for Perceval.

    This class allows the fetch the issues stored in GitHub
    repository.

    :param owner: GitHub owner
    :param repository: GitHub repository from the owner
    :param api_token: GitHub auth token to access the API
    :param base_url: GitHub URL in enterprise edition case;
        when no value is set the backend will be fetch the data
        from the GitHub public site.
    :param tag: label used to mark the data
    :param cache: use issues already retrieved in cache
    :param sleep_for_rate: sleep until rate limit is reset
    :param min_rate_to_sleep: minimun rate needed to sleep until
         it will be reset
    """
    version = '0.10.2'

    def __init__(self, owner=None, repository=None,
                 api_token=None, base_url=None,
                 tag=None, cache=None,
                 sleep_for_rate=False, min_rate_to_sleep=MIN_RATE_LIMIT):
        origin = base_url if base_url else GITHUB_URL
        origin = urijoin(origin, owner, repository)

        super().__init__(origin, tag=tag, cache=cache)
        self.owner = owner
        self.repository = repository
        self.api_token = api_token
        self.client = GitHubClient(owner, repository, api_token, base_url,
                                   sleep_for_rate, min_rate_to_sleep)
        self._users = {}  # internal users cache

    @metadata
    def fetch(self, from_date=DEFAULT_DATETIME):
        """Fetch the issues from the repository.

        The method retrieves, from a GitHub repository, the issues
        updated since the given date.

        :param from_date: obtain issues updated since this date

        :returns: a generator of issues
        """

        self._purge_cache_queue()

        from_date = datetime_to_utc(from_date)

        issues_groups = self.client.get_issues(start=from_date)

        for raw_issues in issues_groups:
            self._push_cache_queue('{ISSUES}')
            self._push_cache_queue(raw_issues)
            self._flush_cache_queue()
            issues = json.loads(raw_issues)
            for issue in issues:
                self.__init_extra_issue_fields(issue)
                for field in TARGET_ISSUE_FIELDS:

                    if not issue[field]:
                        continue

                    if field == 'user':
                        issue[field + '_data'] = self.__get_user(issue[field]['login'])
                    elif field == 'assignee':
                        issue[field + '_data'] = self.__get_issue_assignee(issue[field])
                    elif field == 'assignees':
                        issue[field + '_data'] = self.__get_issue_assignees(issue[field])
                    elif field == 'comments':
                        issue[field + '_data'] = self.__get_issue_comments(issue['number'])
                    elif field == 'reactions':
                        issue[field + '_data'] = \
                            self.__get_issue_reactions(issue['number'], issue['reactions']['total_count'])

                self._push_cache_queue('{ISSUE-END}')
                self._flush_cache_queue()
                yield issue
        self._push_cache_queue('{}{}')
        self._flush_cache_queue()

    @metadata
    def fetch_from_cache(self):
        """Fetch the issues from the cache.
        It returns the issues stored in the cache object provided during
        the initialization of the object. If this method is called but
        no cache object was provided, the method will raise a `CacheError`
        exception.
        :returns: a generator of items
        :raises CacheError: raised when an error occurs accessing the
            cache
        """
        if not self.cache:
            raise CacheError(cause="cache instance was not provided")

        cache_items = self.cache.retrieve()
        raw_item = next(cache_items)

        while raw_item != '{}{}':

            if raw_item == '{ISSUES}':
                issues = self.__fetch_issues_from_cache(cache_items)

            for issue in issues:
                self.__init_extra_issue_fields(issue)
                raw_item = next(cache_items)

                while raw_item != '{ISSUE-END}':
                    try:
                        if raw_item == '{USER}':
                            issue['user_data'] = \
                                self.__fetch_user_and_organization_from_cache(issue['user']['login'], cache_items)
                        elif raw_item == '{ASSIGNEE}':
                            assignee = self.__fetch_assignee_from_cache(cache_items)
                            issue['assignee_data'] = assignee
                        elif raw_item == '{ASSIGNEES}':
                            assignees = self.__fetch_assignees_from_cache(cache_items)
                            issue['assignees_data'] = assignees
                        elif raw_item == '{COMMENTS}':
                            comments = self.__fetch_comments_from_cache(cache_items)
                            issue['comments_data'] = comments
                        elif raw_item == '{ISSUE-REACTIONS}':
                            reactions = self.__fetch_issue_reactions_from_cache(cache_items)
                            issue['reactions_data'] = reactions

                        raw_item = next(cache_items)

                    except StopIteration:
                        # this should be never executed, the while condition prevents
                        # to trigger the StopIteration exception
                        break

                raw_item = next(cache_items)
                yield issue

    def __get_issue_reactions(self, issue_number, total_count):
        """Get issue reactions"""

        reactions = []
        self._push_cache_queue('{ISSUE-REACTIONS}')
        self._flush_cache_queue()

        if total_count == 0:
            self._push_cache_queue('[]')
            self._flush_cache_queue()
            return reactions

        group_reactions = self.client.get_issue_reactions(issue_number)

        for raw_reactions in group_reactions:
            self._push_cache_queue(raw_reactions)
            self._flush_cache_queue()

            for reaction in json.loads(raw_reactions):
                reaction['user_data'] = self.__get_user(reaction['user']['login'])
                reactions.append(reaction)

        return reactions

    def __get_issue_comments(self, issue_number):
        """Get issue comments"""

        comments = []
        group_comments = self.client.get_issue_comments(issue_number)
        self._push_cache_queue('{COMMENTS}')
        self._flush_cache_queue()

        for raw_comments in group_comments:
            self._push_cache_queue(raw_comments)
            self._flush_cache_queue()

            for comment in json.loads(raw_comments):
                comment_id = comment.get('id')
                comment['user_data'] = self.__get_user(comment['user']['login'])
                comment['reactions_data'] = \
                    self.__get_issue_comment_reactions(comment_id, comment['reactions']['total_count'])
                comments.append(comment)

        return comments

    def __get_issue_comment_reactions(self, comment_id, total_count):
        """Get reactions on issue comments"""

        reactions = []
        self._push_cache_queue('{COMMENT-REACTIONS}')
        self._flush_cache_queue()

        if total_count == 0:
            self._push_cache_queue('[]')
            self._flush_cache_queue()
            return reactions

        group_reactions = self.client.get_issue_comment_reactions(comment_id)

        for raw_reactions in group_reactions:
            self._push_cache_queue(raw_reactions)
            self._flush_cache_queue()

            for reaction in json.loads(raw_reactions):
                reaction['user_data'] = self.__get_user(reaction['user']['login'])
                reactions.append(reaction)

        return reactions

    def __get_issue_assignee(self, raw_assignee):
        """Get issue assignee"""

        self._push_cache_queue('{ASSIGNEE}')
        self._push_cache_queue(raw_assignee)
        self._flush_cache_queue()
        assignee = self.__get_user(raw_assignee['login'])

        return assignee

    def __get_issue_assignees(self, raw_assignees):
        """Get issue assignees"""

        self._push_cache_queue('{ASSIGNEES}')
        self._push_cache_queue(raw_assignees)
        self._flush_cache_queue()
        assignees = []
        for ra in raw_assignees:
            assignees.append(self.__get_user(ra['login']))

        return assignees

    def __get_user(self, login):
        """Get user and org data for the login"""

        user = {}

        if not login:
            return user

        user_raw = self.client.get_user(login)
        user = json.loads(user_raw)
        self._push_cache_queue('{USER}')
        self._push_cache_queue(user_raw)
        user_orgs_raw = \
            self.client.get_user_orgs(login)
        user['organizations'] = json.loads(user_orgs_raw)
        self._push_cache_queue(user_orgs_raw)
        self._flush_cache_queue()

        return user

    def __fetch_issues_from_cache(self, cache_items):
        """Fetch issues from cache"""

        raw_content = next(cache_items)
        issues = json.loads(raw_content)
        return issues

    def __fetch_user_and_organization_from_cache(self, user_login, cache_items):
        """Fetch user and organization from cache"""

        raw_user = next(cache_items)
        raw_org = next(cache_items)
        return self.__get_user_and_organization(user_login, raw_user, raw_org)

    def __fetch_assignee_from_cache(self, cache_items):
        """Fetch issue assignee from cache"""

        raw_assignee = next(cache_items)
        user_tag = next(cache_items)
        raw_user = next(cache_items)
        raw_org = next(cache_items)
        assignee = self.__get_user_and_organization(raw_assignee['login'], raw_user, raw_org)

        return assignee

    def __fetch_assignees_from_cache(self, cache_items):
        """Fetch issue assignees from cache"""

        raw_assignees = next(cache_items)
        assignees = []
        for a in raw_assignees:
            user_tag = next(cache_items)
            raw_user = next(cache_items)
            raw_org = next(cache_items)
            a = self.__get_user_and_organization(a['login'], raw_user, raw_org)
            assignees.append(a)

        return assignees

    def __fetch_issue_comment_reactions_from_cache(self, cache_items):
        """Fetch issue comment reactions from cache"""

        raw_content = next(cache_items)
        reactions = json.loads(raw_content)
        for reaction in reactions:
            user_tag = next(cache_items)
            raw_user = next(cache_items)
            raw_org = next(cache_items)
            reaction['user_data'] = self.__get_user_and_organization(reaction['user']['login'], raw_user, raw_org)

        return reactions

    def __fetch_issue_reactions_from_cache(self, cache_items):
        """Fetch issue reactions from cache"""

        raw_content = next(cache_items)
        reactions = json.loads(raw_content)
        for r in reactions:
            user_tag = next(cache_items)
            raw_user = next(cache_items)
            raw_org = next(cache_items)
            r['user_data'] = self.__get_user_and_organization(r['user']['login'], raw_user, raw_org)

        return reactions

    def __fetch_comments_from_cache(self, cache_items):
        """Fetch issue comments from cache"""

        raw_content = next(cache_items)
        comments = json.loads(raw_content)
        for c in comments:
            user_tag = next(cache_items)
            raw_user = next(cache_items)
            raw_org = next(cache_items)
            c['user_data'] = self.__get_user_and_organization(c['user']['login'], raw_user, raw_org)

            reactions_tag = next(cache_items)
            reactions = self.__fetch_issue_comment_reactions_from_cache(cache_items)
            c['reactions_data'] = reactions

        return comments

    def __init_extra_issue_fields(self, issue):
        """Add fields to an issue"""

        issue['user_data'] = {}
        issue['assignee_data'] = {}
        issue['assignees_data'] = []
        issue['comments_data'] = []
        issue['reactions_data'] = []

    def __get_user_and_organization(self, login, raw_user, raw_org):
        found = self._users.get(login)

        if not found:
            user = json.loads(raw_user)
            user['organizations'] = json.loads(raw_org)
            self._users.update({login: user})
            found = self._users.get(login)

        return found

    @classmethod
    def has_caching(cls):
        """Returns whether it supports caching items on the fetch process.

        :returns: this backend supports items cache
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
        """Extracts the identifier from a GitHub item."""

        return str(item['id'])

    @staticmethod
    def metadata_updated_on(item):
        """Extracts the update time from a GitHub item.

        The timestamp used is extracted from 'updated_at' field.
        This date is converted to UNIX timestamp format. As GitHub
        dates are in UTC the conversion is straightforward.

        :param item: item generated by the backend

        :returns: a UNIX timestamp
        """
        ts = item['updated_at']
        ts = str_to_datetime(ts)

        return ts.timestamp()

    @staticmethod
    def metadata_category(item):
        """Extracts the category from a GitHub item.

        This backend only generates one type of item which is
        'issue'.
        """
        return 'issue'


class GitHubClient:
    """Client for retieving information from GitHub API"""

    _users = {}       # users cache
    _users_orgs = {}  # users orgs cache

    def __init__(self, owner, repository, token, base_url=None,
                 sleep_for_rate=False, min_rate_to_sleep=MIN_RATE_LIMIT):
        self.owner = owner
        self.repository = repository
        self.token = token
        self.rate_limit = None
        self.rate_limit_reset_ts = None
        self.sleep_for_rate = sleep_for_rate

        if base_url:
            parts = urllib.parse.urlparse(base_url)
            self.api_url = parts.scheme + '://' + 'api.' + parts.netloc
        else:
            self.api_url = GITHUB_API_URL

        if min_rate_to_sleep > MAX_RATE_LIMIT:
            msg = "Minimum rate to sleep value exceeded (%d)."
            msg += "High values might cause the client to sleep forever."
            msg += "Reset to %d."
            logger.warning(msg, min_rate_to_sleep, MAX_RATE_LIMIT)

        self.min_rate_to_sleep = min(min_rate_to_sleep, MAX_RATE_LIMIT)

    def get_issue_reactions(self, issue_number):
        """Get reactions of an issue"""

        return self._fetch_items("/issues/" + str(issue_number) + "/reactions", "comment")

    def get_issue_comment_reactions(self, comment_id):
        """Get reactions of an issue comment"""

        return self._fetch_items("/issues/comments/" + str(comment_id) + "/reactions", "comment")

    def get_issue_comments(self, issue_number):
        """Get the issue comments from pagination"""

        return self._fetch_items("/issues/" + str(issue_number) + "/comments", "comment")

    def get_issues(self, start=None):
        """Get the issues from pagination"""

        return self._fetch_items("/issues", payload_type="issue", start=start)

    def get_user(self, login):
        """Get the user information and update the user cache"""

        user = None

        if login in self._users:
            return self._users[login]

        url_user = urijoin(self.api_url, 'users', login)

        logging.info("Getting info for %s" % (url_user))
        r = self.__send_request(url_user, headers=self.__get_headers())
        user = r.text
        self._users[login] = user

        return user

    def get_user_orgs(self, login):
        """Get the user public organizations"""

        if login in self._users_orgs:
            return self._users_orgs[login]

        url = urijoin(self.api_url, 'users', login, 'orgs')
        try:
            r = self.__send_request(url, headers=self.__get_headers())
            orgs = r.text
        except requests.exceptions.HTTPError as ex:
            # 404 not found is wrongly received sometimes
            if ex.response.status_code == 404:
                logger.error("Can't get github login orgs: %s", ex)
                orgs = '[]'
            else:
                raise ex

        self._users_orgs[login] = orgs

        return orgs

    def __get_url_repo(self):
        """Build URL repo"""

        github_api_repos = urijoin(self.api_url, 'repos')
        url_repo = urijoin(github_api_repos, self.owner, self.repository)
        return url_repo

    def __get_url(self, path, startdate=None):
        """Build repo-"related URL"""

        url = self.__get_url_repo() + path
        return url

    def __get_payload(self, payload_type='issue', startdate=None):
        """Build payload"""

        # 100 in other items. 20 for pull requests. 30 issues
        payload = {'per_page': 30,
                   'direction': 'asc'}
        if payload_type == 'issue':
            payload['state'] = 'all'
            payload['sort'] = 'updated'
        elif payload_type == 'comment':
            payload['sort'] = 'updated'

        if startdate:
            startdate = startdate.isoformat()
            payload['since'] = startdate
        return payload

    def __get_headers(self):
        """Set header for request"""

        if self.token:
            headers = {'Authorization': 'token ' + self.token,
                       'Accept': 'application/vnd.github.squirrel-girl-preview'}
            return headers

    def __send_request(self, url, params=None, headers=None):
        """GET HTTP caring of rate limit"""

        if self.rate_limit is not None and self.rate_limit <= self.min_rate_to_sleep:
            seconds_to_reset = self.rate_limit_reset_ts - int(time.time()) + 1
            cause = "GitHub rate limit exhausted."
            if self.sleep_for_rate:
                logger.info("%s Waiting %i secs for rate limit reset.", cause, seconds_to_reset)
                time.sleep(seconds_to_reset)
            else:
                raise RateLimitError(cause=cause, seconds_to_reset=seconds_to_reset)

        r = requests.get(url, params=params, headers=headers)
        r.raise_for_status()

        # GitHub service establishes API rate limits.
        # Enterprise version might have them deactivated.
        if 'X-RateLimit-Remaining' in r.headers:
            self.rate_limit = int(r.headers['X-RateLimit-Remaining'])
            self.rate_limit_reset_ts = int(r.headers['X-RateLimit-Reset'])
            logger.debug("Rate limit: %s", self.rate_limit)
        else:
            self.rate_limit = None
            self.rate_limit_reset_ts = None

        return r

    def _fetch_items(self, path, payload_type, start=None):
        """Return the items from github API using links pagination"""

        page = 0  # current page
        last_page = None  # last page
        url_next = self.__get_url(path, start)

        logger.debug("Get GitHub paginated items from " + url_next)
        payload = self.__get_payload(payload_type, start)
        r = self.__send_request(url_next, payload, self.__get_headers())
        items = r.text
        page += 1

        if 'last' in r.links:
            last_url = r.links['last']['url']
            last_page = last_url.split('&page=')[1].split('&')[0]
            last_page = int(last_page)
            logger.debug("Page: %i/%i" % (page, last_page))

        while items:
            yield items

            items = None

            if 'next' in r.links:
                url_next = r.links['next']['url']  # Loving requests :)
                r = self.__send_request(url_next, payload, self.__get_headers())
                page += 1
                items = r.text
                logger.debug("Page: %i/%i" % (page, last_page))


class GitHubCommand(BackendCommand):
    """Class to run GitHub backend from the command line."""

    BACKEND = GitHub

    @staticmethod
    def setup_cmd_parser():
        """Returns the GitHub argument parser."""

        parser = BackendCommandArgumentParser(from_date=True,
                                              token_auth=True,
                                              cache=True)

        # GitHub options
        group = parser.parser.add_argument_group('GitHub arguments')
        group.add_argument('--enterprise-url', dest='base_url',
                           help="Base URL for GitHub Enterprise instance")
        group.add_argument('--sleep-for-rate', dest='sleep_for_rate',
                           action='store_true',
                           help="sleep for getting more rate")
        group.add_argument('--min-rate-to-sleep', dest='min_rate_to_sleep',
                           default=MIN_RATE_LIMIT, type=int,
                           help="sleep until reset when the rate limit reaches this value")

        # Positional arguments
        parser.parser.add_argument('owner',
                                   help="GitHub owner")
        parser.parser.add_argument('repository',
                                   help="GitHub repository")

        return parser
