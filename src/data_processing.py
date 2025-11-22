# |----------------------- IMPORTS -----------------------|

from pathlib import Path
import json
import pandas as pd
import numpy as np


# |----------------------METHOD DEFINITIONS------------------------|


def load_raw_data(raw_data_dir: str = '../data/raw') -> dict:
    """
    Loads raw JSON data from individual repo files in the specified directory 
    and returns a dictionary of data keyed by repository name.
    Excludes files that do not correspond to individual repositories.
    """
    raw_data_path = Path(raw_data_dir)
    raw_json_files = list(raw_data_path.glob('*.json'))
    repo_data = {}

    for file in raw_json_files:
        # Check the file name stem to ensure it's not the combined file.
        if file.stem.startswith('combined_data'):
            continue  # Skip the combined file

        with open(file, 'r') as f:
            data = json.load(f)

        # Extract filename to use as key in repo_data:
        repo_name = file.stem.rsplit('_', 2)[0].replace('_', '/', 1)
        repo_data[repo_name] = data

    return repo_data


def load_cleaned_data(cleaned_data_dir: str = '../data/cleaned') -> dict:
    """
    Loads cleaned CSV data files and returns a dictionary containing 
    DataFrames for commits, issues, and info with proper date parsing.

    Returns:
        dict: Dictionary with keys 'commits_df', 'issues_df', 'info_df'
    """
    data_path = Path(cleaned_data_dir)
    data_files = list(data_path.glob('*.csv'))

    cleaned_data = {}

    for file in data_files:
        if 'commits' in file.name:
            cleaned_data['commits_df'] = pd.read_csv(
                data_path / file.name,
                parse_dates=['author_date', 'commit_date']
            )
        elif 'issues' in file.name:
            cleaned_data['issues_df'] = pd.read_csv(
                data_path / file.name,
                parse_dates=['created_at', 'closed_at', 'updated_at']
            )
        elif 'info' in file.name:
            cleaned_data['info_df'] = pd.read_csv(
                data_path / file.name,
                parse_dates=['created_at', 'updated_at', 'pushed_at']
            )

    # Verify all expected dataframes were loaded
    expected_keys = ['commits_df', 'issues_df', 'info_df']
    missing_keys = [key for key in expected_keys if key not in cleaned_data]

    if missing_keys:
        print(f"Warning: Missing expected data files: {missing_keys}")

    return cleaned_data


def check_repo_structure(repo_name: str, repo_data: dict) -> dict:
    """Check the Structure of a Repository to help determine how to approach analysis."""

    repo_to_examine = repo_name
    repo_pd = repo_data.get(repo_to_examine)

    if repo_pd is not None:
        print(f"--- Data Loaded for: {repo_to_examine} ---")
        print("Keys in the data:")
        print(repo_pd.keys())
        print(f'\nNumber of issues: {len(repo_pd["issues"])}')
        print(f'Number of commits: {len(repo_pd["commits"])}')
    else:
        print(f"Error: {repo_to_examine} data not found in repo_data.")

    return repo_pd


def extract_issues(repo_data):
    """Extract and flatten issues from all repositories"""
    all_issues = []

    for repo_name, data in repo_data.items():
        if 'combined' in repo_name:
            continue
        issues = data.get('issues', [])

        for issue in issues:

            flat_issue = {
                'repo_name': repo_name,
                'issue_id': issue.get('id'),
                'issue_number': issue.get('number'),
                'title': issue.get('title'),
                'state': issue.get('state'),
                'user_login': issue.get('user', {}).get('login'),
                'created_at': issue.get('created_at'),
                'updated_at': issue.get('updated_at'),
                'closed_at': issue.get('closed_at'),
                'comments': issue.get('comments'),
                'author_association': issue.get('author_association'),
                # Count of labels
                'labels_count': len(issue.get('labels', []))
            }
            all_issues.append(flat_issue)
    return pd.DataFrame(all_issues)


def extract_commits(repo_data):
    """Extract and flatten commits from all repositories"""
    all_commits = []

    for repo_name, data in repo_data.items():
        # Skip the combined data file
        if 'combined' in repo_name:
            continue

        commits = data.get('commits', [])

        for commit in commits:
            commit_data = commit.get('commit', {})
            author_data = commit_data.get('author', {})

            flat_commit = {
                'repo_name': repo_name,
                'sha': commit.get('sha'),
                'author_name': author_data.get('name'),
                'author_email': author_data.get('email'),
                'author_date': author_data.get('date'),
                'committer_name': commit_data.get('committer', {}).get('name'),
                'commit_date': commit_data.get('committer', {}).get('date'),
                'description': commit_data.get('description'),
                'message': commit_data.get('message'),
                'message_length': len(commit_data.get('message', '')),
            }
            all_commits.append(flat_commit)

    return pd.DataFrame(all_commits)


def extract_info(repo_data):
    """Extract and flatten info"""
    all_info = []

    for repo_name, data in repo_data.items():
        if 'combined' in repo_name:
            print(f'Skipping: {repo_name}')
            continue
        info = data.get('info', None)

        flat_info = {
            'repo_name': repo_name,
            'stars': info.get('stargazers_count'),
            'forks': info.get('forks_count'),
            'open_issues': info.get('open_issues_count'),
            'language': info.get('language'),
            'description': info.get('description'),
            'created_at': info.get('created_at'),
            'updated_at': info.get('updated_at'),
            'pushed_at': info.get('pushed_at')
        }
        all_info.append(flat_info)

    return pd.DataFrame(all_info)


# define save methd:

def save_data(dataframe, filename):
    """Save data to csv file in the data/clean directory"""
    clean_data_dir = Path('../data/cleaned')
    clean_data_dir.mkdir(parents=True, exist_ok=True)

    filepath = clean_data_dir/filename

    dataframe.to_csv(filepath, index=False)

    print(f"Saved to {filepath}")
