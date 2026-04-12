"""
Defines configuration constants for GitHub API interactions in the analytics module.
Includes settings for API endpoints, authentication, and rate limiting to ensure efficient and reliable data retrieval from
"""
import os

from dotenv import load_dotenv

load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
BASE_URL = "https://api.github.com"

HTTP_TIMEOUT_SECONDS = float(os.getenv("HTTP_TIMEOUT_SECONDS", 20))
REQUEST_DELAY_SECONDS = float(os.getenv("REQUEST_DELAY_SECONDS", 0.25))
SEARCH_REQUEST_DELAY_SECONDS = 1.0
SECONDARY_RATE_LIMIT_FALLBACK_SECONDS = 30
