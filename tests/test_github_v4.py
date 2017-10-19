import requests
import json
import re

# Live query can be executed at: https://developer.github.com/v4/explorer/

TOKEN = "XXX"
GRAPHQL_GITHUB_URL = "https://api.github.com/graphql"

SIMPLE_QUERY = """
   query 
   { 
            repository(owner: "gabrielecirulli", name: "2048") { 
                createdAt 
            }
   }
   """

QUERY_ISSUES = """
    query 
    {
          repository(owner: "gabrielecirulli", name: "2048") {
            issues(first: 30) {
              edges {
                node {
                  author {
                    login
                  }
                  number
                  id
                  body
                  closed
                }
              }
            }
          }
    }
    """

QUERY_ORDERED_ISSUES_FIRST = """
    query($order:IssueOrder!)
    {
      repository(owner: "gabrielecirulli", name: "2048") {
        issues(first: 30, orderBy : $order  ) {
          edges {
            node {
              author {
                login
              }
              createdAt
              updatedAt
              number
              id
              body
              closed
            }
          }
        }
      }
    }
    """

QUERY_ORDERED_ISSUES_NEXT = """
    query($order:IssueOrder!)
    {
      repository(owner: "gabrielecirulli", name: "2048") {
        issues(first: 30, <AFTER> orderBy : $order  ) {
          edges {
            node {
              author {
                login
              }
              createdAt
              updatedAt
              number
              id
              body
              closed
            }
            cursor
          }
        }
      }
    }
    """

VARIABLES_ORDERED_ISSUES = """
    {"order":{"field":"UPDATED_AT", "direction":"ASC"}}
    """


def build_header():
    headers = {'Authorization': 'bearer ' + TOKEN}
    return headers


def digest_raw_text(raw_text):
    return re.sub("\s+", " ", raw_text).replace('"', '\\"')


def send_request(payload):
    r = requests.post(GRAPHQL_GITHUB_URL, data=payload, headers=build_header())
    return r.text


def prepare_payload(query, variables=None):
    payload = '{"query":"' + digest_raw_text(query) + '"'
    if variables:
        payload += ', "variables": "' + digest_raw_text(variables) + '"'

    return str(payload) + "}"


def execute(query, variables=None, show=False):
    payload = prepare_payload(query, variables)
    raw_answer = send_request(payload)
    answer = json.loads(raw_answer)

    if show:
        print(json.dumps(answer, sort_keys=True, indent=4))

    return answer


def replace_tagged_text(text, tag, replacement):
    return re.sub(tag, replacement, text, count=1)


def fetch_issues():
    issues = []
    query = replace_tagged_text(QUERY_ORDERED_ISSUES_NEXT, "<AFTER>", "")

    fetch_data = True
    while fetch_data:
        answer = execute(query, VARIABLES_ORDERED_ISSUES)
        issue_nodes = answer['data']['repository']['issues']['edges']

        if not issue_nodes:
            fetch_data = False
            continue

        issues.extend([n['node'] for n in issue_nodes])
        last_edge = issue_nodes[-1]
        last_cursor = last_edge['cursor']
        query = replace_tagged_text(QUERY_ORDERED_ISSUES_NEXT, "<AFTER>", 'after: "' + str(last_cursor) + '",')

    print("Number of issues: " + str(len(issues)))

    return issues


def main():
    execute(SIMPLE_QUERY, show=True)
    execute(QUERY_ISSUES, show=True)
    execute(QUERY_ORDERED_ISSUES_FIRST, VARIABLES_ORDERED_ISSUES, show=True)
    fetch_issues()


if __name__ == "__main__":
    main()