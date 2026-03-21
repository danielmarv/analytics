"""
GraphQL queries to later be used for fetching data from GitHub
"""


REPOS_QUERY: str = """
query($org:String!,$cursor:String){
  organization(login:$org){
    repositories(first:100, after:$cursor){
      pageInfo{
        hasNextPage
        endCursor
      }
      nodes{
        name
      }
    }
  }
  rateLimit{
    limit
    remaining
    cost
    resetAt
  }
}
"""

ISSUES_QUERY: str = """
query($owner:String!,$repo:String!,$cursor:String,$states:[IssueState!]){
  repository(owner:$owner,name:$repo){
    issues(first:100, after:$cursor, states:$states){
      pageInfo{
        hasNextPage
        endCursor
      }
      nodes{
        number
        title
        state
        createdAt
        closedAt
        labels(first:10){
          nodes{
            name
          }
        }
      }
    }
  }
  rateLimit{
    limit
    remaining
    cost
    resetAt
  }
}
"""

MERGED_PR_QUERY: str = """
query($owner:String!, $repo:String!, $cursor:String) {
  repository(owner:$owner, name:$repo) {
    pullRequests(
      first:100
      after:$cursor
      states:MERGED
      orderBy:{field:UPDATED_AT, direction:DESC}
    ) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        number
        createdAt
        mergedAt
        additions
        deletions
        changedFiles
        closingIssuesReferences(first:10) {
          nodes {
            number
            labels(first:10) {
              nodes {
                name
              }
            }
          }
        }
      }
    }
  }
  rateLimit{
    limit
    remaining
    cost
    resetAt
  }
}
"""

CONTRIBUTOR_ISSUE_ACTIVITY_QUERY: str = """
query($owner:String!, $repo:String!, $cursor:String) {
  repository(owner:$owner, name:$repo) {
    issues(
      first:100
      after:$cursor
      orderBy:{field:CREATED_AT, direction:ASC}
    ) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        number
        createdAt
        author {
          login
        }
        comments(first:100) {
          nodes {
            createdAt
            author {
              login
            }
          }
        }
        timelineItems(
          first:100
          itemTypes:[LABELED_EVENT, UNLABELED_EVENT, CLOSED_EVENT, REOPENED_EVENT, ASSIGNED_EVENT]
        ) {
          nodes {
            __typename
            ... on LabeledEvent {
              createdAt
              actor {
                login
              }
              label {
                name
              }
            }
            ... on UnlabeledEvent {
              createdAt
              actor {
                login
              }
              label {
                name
              }
            }
            ... on ClosedEvent {
              createdAt
              actor {
                login
              }
            }
            ... on ReopenedEvent {
              createdAt
              actor {
                login
              }
            }
            ... on AssignedEvent {
              createdAt
              actor {
                login
              }
              assignee {
                __typename
                ... on User {
                  login
                }
              }
            }
          }
        }
      }
    }
  }
  rateLimit {
    limit
    remaining
    cost
    resetAt
  }
}
"""

CONTRIBUTOR_PULL_REQUEST_ACTIVITY_QUERY: str = """
query($owner:String!, $repo:String!, $cursor:String) {
  repository(owner:$owner, name:$repo) {
    pullRequests(
      first:100
      after:$cursor
      orderBy:{field:CREATED_AT, direction:ASC}
    ) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        number
        createdAt
        mergedAt
        author {
          login
        }
        mergedBy {
          login
        }
        comments(first:100) {
          nodes {
            createdAt
            author {
              login
            }
          }
        }
        reviews(first:100) {
          nodes {
            createdAt
            state
            author {
              login
            }
          }
        }
        timelineItems(
          first:100
          itemTypes:[LABELED_EVENT, UNLABELED_EVENT, CLOSED_EVENT, REOPENED_EVENT]
        ) {
          nodes {
            __typename
            ... on LabeledEvent {
              createdAt
              actor {
                login
              }
              label {
                name
              }
            }
            ... on UnlabeledEvent {
              createdAt
              actor {
                login
              }
              label {
                name
              }
            }
            ... on ClosedEvent {
              createdAt
              actor {
                login
              }
            }
            ... on ReopenedEvent {
              createdAt
              actor {
                login
              }
            }
          }
        }
      }
    }
  }
  rateLimit {
    limit
    remaining
    cost
    resetAt
  }
}
"""
