import requests

from grimoirelab_toolkit.uris import urijoin


def main():
    API_URL = 'https://archive.softwareheritage.org/'

    TARGET_REPO = 'https://github.com/python/cpython/'

    url = urijoin(API_URL, 'api/1/origin/git/url', TARGET_REPO)
    response = requests.get(url)
    repo_info = response.json()

    visits_url = repo_info['origin_visits_url']

    url = urijoin(API_URL, visits_url)
    response = requests.get(url)
    visits_info = response.json()

    for visit_info in visits_info:
        snapshot_url = visit_info['snapshot_url']
        url = urijoin(API_URL, snapshot_url)
        response = requests.get(url)
        snapshot_info = response.json()

        
        id = snapshot_info['id']

        url = urijoin(API_URL, 'api/1/content/sha1:' + id, 'license')
        response = requests.get(url)
        license_info = response.json()
        print("here")

    print("here")


if __name__ == '__main__':
    main()