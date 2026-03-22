"""GraphQL queries for contributor responsibility activity."""

CONTRIBUTOR_ISSUE_ACTIVITY_QUERY: str = """
query($owner:String!, $repo:String!, $cursor:String) {
  repository(owner:$owner, name:$repo) {
    issues(
      first:100
      after:$cursor
      orderBy:{field:UPDATED_AT, direction:DESC}
    ) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        number
        createdAt
        updatedAt
        author {
          login
        }
        timelineItems(
          first:100
          itemTypes:[LABELED_EVENT, UNLABELED_EVENT, ASSIGNED_EVENT]
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
      orderBy:{field:UPDATED_AT, direction:DESC}
    ) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        number
        createdAt
        updatedAt
        mergedAt
        author {
          login
        }
        mergedBy {
          login
        }
        reviews(first:100) {
          nodes {
            createdAt
            author {
              login
            }
          }
        }
        timelineItems(
          first:100
          itemTypes:[LABELED_EVENT, UNLABELED_EVENT]
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
