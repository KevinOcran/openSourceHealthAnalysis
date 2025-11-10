"""
GitHub API Data Collection Script
Fetches issues and commits from specified open-source repositories
"""

import requests
import json
import os
from datetime import datetime
from pathlib import Path
import time

# get API token from environment variable
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')

if not GITHUB_TOKEN:
    raise ValueError('GITHUB_TOKEN not found !! Set it in the .envrc file')

# API config:
BASE_URL = "https://api.github.com"
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# Set repositories to be analyzed:
REPOSITORIES = [
    "pandas-dev/pandas",
    "numpy/numpy",
    "scikit-learn/scikit-learn",
    "apache/airflow",
    "mlflow/mlflow"
]


def check_rate_limit():
    """Check GitHub API rate limit status"""
    response = requests.get(f"{BASE_URL}/rate_limit", headers=HEADERS)
    if response.status_code == 200:
        data = response.json()
        core = data['resources']['core']
        print(
            f"Rate limit: {core['remaining']} remaining out of {core['limit']}")
        print(f"Reset time: {datetime.fromtimestamp(core['reset'])}")
        return core['remaining']
    return 0


def fetch_issues(repo, max_issues=100):
    """
    Fetch issues from a repository

    Args:
        repo: Repository in format 'owner/name'
        max_issues: Maximum number of issues to fetch

    Returns:
        List of issue dictionaries
    """
    issues = []
    page = 1
    per_page = 100

    while len(issues) < max_issues:
        url = f"{BASE_URL}/repos/{repo}/issues"
        params = {
            "state": "all",
            "per_page": per_page,
            "page": page,
            "sort": "created",
            "direction": "desc"
        }

        response = requests.get(url, headers=HEADERS, params=params)

        if response.status_code == 200:
            batch = response.json()
            if not batch or len(batch) == 0:
                break   # No more issues

            # Filter out pull requests:
            actual_issues = [
                issue for issue in batch if 'pull_request' not in issue]
            issues.extend(actual_issues)
            print(
                f'Fetched {len(actual_issues)} issues from page {page} of {repo} so far...')
            page += 1

            if len(issues) >= max_issues:
                break
        else:
            print(f"  Error fetching issues: {response.status_code}")
            print(
                f"  Message: {response.json().get('message', 'Unknown error')}")
            break

        time.sleep(0.5)  # Respect API
    return issues[:max_issues]


def fetch_commits(repo, max_commits=100):
    """
    Fetch commits from a repository
    Args:
        repo: Repository in format 'owner/name'
        max_commits: Maximum number of commits to fetch

    Returns:
        List of commit dictionaries
    """
    print(f'\nFetching commits for {repo}...')
    commits = []
    page = 1
    per_page = 100

    while len(commits) < max_commits:
        url = f"{BASE_URL}/repos/{repo}/commits"
        params = {
            "per_page": per_page,
            "page": page
        }

        response = requests.get(url, headers=HEADERS, params=params)

        if response.status_code == 200:
            batch = response.json()
            if not batch or len(batch) == 0:
                break

            commits.extend(batch)
            print(f"  Fetched {len(commits)} commits so far...")
            page += 1

            if len(batch) < per_page:
                break
        else:
            print(f"  Error fetching commits: {response.status_code}")
            print(
                f"  Message: {response.json().get('message', 'Unknown error')}")
            break
        time.sleep(0.5)  # Respect API
    return commits[:max_commits]


def fetch_repo_info(repo):
    """Fetch general repository information"""
    url = f"{BASE_URL}/repos/{repo}"

    response = requests.get(url, headers=HEADERS)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching repository info {response.status_code}")
        return None


def save_data(data, filename):
    """Save data to JSON file in the data/raw directory"""
    raw_data_dir = Path("data/raw")
    raw_data_dir.mkdir(parents=True, exist_ok=True)

    filepath = raw_data_dir / filename
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"Saved to {filepath}")


def main():
    """Main execution function"""

    print("="*60)
    print("GitHub API Data Collection")
    print("="*60)

    # Check rate limit before starting:
    remainig = check_rate_limit()
    if remainig < 100:
        print("Warning: Low rate limit remaining. Consider waiting.")
        return

    # Create timestamp for this collection run:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Collect data for each repo:

    all_data = {}

    for repo in REPOSITORIES:
        print(f"\n{'='*60}")
        print(f"Processing: {repo}")
        print(f"{'='*60}")

        repo_data = {
            "repository": repo,
            "collected at": datetime.now().isoformat(),
            "info": None,
            "issues": [],
            "commits": []
        }

        # fetch repo info:
        repo_data['info'] = fetch_repo_info(repo)

        # fetch issues:
        repo_data['issues'] = fetch_issues(repo, max_issues=100)

        # fetch commits:
        repo_data['commits'] = fetch_commits(repo, max_commits=100)

        # store individual repo data:
        all_data[repo] = repo_data

        repo_name = repo.replace('/', '_')
        save_data(repo_data, f'{repo_name}_{timestamp}.json')

        # check rate limit after each repo:
        remaining = check_rate_limit()
        if remaining < 50:
            print("\nWARNING: Low rate limit. Stopping further requests.")
            break

    # Save all data together:
    save_data(all_data, f'all_repos_{timestamp}.json')

    print(f"\n{'='*60}")
    print("Data collection complete!")
    print(f"{'='*60}")
    print(f"\nSummary:")

    for repo, data in all_data.items():
        print(f"\nRepository: {repo}")
        print(f"  Issues collected: {len(data['issues'])}")
        print(f"  Commits collected: {len(data['commits'])}")


if __name__ == "__main__":
    main()
