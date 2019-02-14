import json

from perceval.backends.core.github import (GitHub,
                                           GitHubCommand,
                                           GitHubClient,
                                           CATEGORY_ISSUE,
                                           CATEGORY_PULL_REQUEST)
from perceval.backends.core.gharchive import (GHArchive, CATEGORY_EVENTS)

from grimoirelab_toolkit.datetime import str_to_datetime

def main():
    # from perceval.archive import Archive
    #
    # archive = Archive.create('/home/slimbook/Escritorio/github-issue-live.archive')
    # backend = GitHub("grimoirelab", "perceval", api_token="60a8ce828f727495b41e77c60bde8d06cfd31e00",
    #                  sleep_for_rate=True, archive=archive)
    # pulls = [pulls for pulls in backend.fetch()]

    owner = 'chaoss'
    repository = 'grimoirelab-perceval'
    conf_path = '/home/slimbook/Escritorio/My Project-1149d6f95034.json'
    g = GHArchive(owner, repository, conf_path)
    from_date = str_to_datetime('2018-12-05')
    for e in g.fetch(from_date=from_date):
        print(json.dumps(e, sort_keys=True, indent=4))


if __name__ == "__main__":
    main()
