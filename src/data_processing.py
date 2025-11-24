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


def compute_issues_summary(issues_df):
    # Group issue data by repo_name and perform summary stats:
    issues_summary = issues_df.groupby('repo_name').agg({
        'issue_id': 'count',
        'issue_age_days': ['mean', 'median']
    }).reset_index()

    issues_summary.columns = [
        f'{top_level}_{bottom_level}' for top_level, bottom_level in issues_summary.columns]
    issues_summary = issues_summary.rename(columns={
        'repo_name_': 'repo_name',
        'issue_id_count': 'number_of_issues',
        'issue_age_days_mean': 'avg_issue_age_days',
        'issue_age_days_median': 'median_issue_age_days'
    })

    return issues_summary


def compute_commits_summary(commits_df):
    # Group commit data by repo_name and perform aggregation:
    commits_summary = commits_df.groupby('repo_name').agg({
        'sha': 'count'
    }).reset_index().rename(columns={'sha': 'commit_count', 'repo_name': 'repo_name'})

    return commits_summary


def compute_info_summary(info_df):
    # Group info data by repo_name and perform aggregation:
    info_summary = info_df.groupby('repo_name').agg({
        'stars': 'sum'
    }).reset_index().rename(columns={'stars': 'stargazers_sum'})

    return info_summary


def compute_closed_issues_summary(issues_df):
    # Create new column, closed_issues to hold the time it took to close issues (only actually closed issues):
    closed_issues = issues_df[['repo_name', 'time_to_close_hours']].dropna(subset=[
                                                                           'time_to_close_hours'])

    # Group time to close issues by 'repo_name' and calculate 'average_time_to_close issues' for each repo:
    closed_issues_summary = closed_issues.groupby('repo_name').agg({
        'time_to_close_hours': 'mean',
    }).reset_index().rename(columns={'time_to_close_hours': 'average_time_to_close_hours'})

    return closed_issues_summary


def compute_resolve_time_comparison(issues_df):
    # Create resolve_time_cmpr to hold the time it took to close issues and the author association of closed issues:
    resolve_time_cmpr = issues_df[['repo_name', 'time_to_close_hours',
                                   'author_association']].dropna(subset=['time_to_close_hours'])

    # Group resolve_time_cmpr by repo_name and author_association and then find the mean and median time for each author_association and repo:
    resolve_time_cmpr = resolve_time_cmpr.groupby(['repo_name', 'author_association']).agg({
        'time_to_close_hours': ['mean', 'median']
    })

    # Flatten new columns:
    resolve_time_cmpr.columns = [
        f'{top_level}_{bottom_level}' for top_level, bottom_level in resolve_time_cmpr.columns]

    resolve_time_cmpr = resolve_time_cmpr.rename(columns={
        'time_to_close_hours_mean': 'avg_time_to_close',
        'time_to_close_hours_median': 'median_time_to_close'
    }).reset_index()

    return resolve_time_cmpr


def compute_commits_by_date(commits_df):
    # Create commits_by_date to store repo_name, author_date and message_length:
    commits_by_date = commits_df[['repo_name',
                                  'author_date', 'message_length']].copy()

    commits_by_date['day_of_the_week'] = commits_by_date['author_date'].dt.day_name()
    commits_by_date['author_date'] = commits_by_date['author_date'].dt.date

    return commits_by_date


def compute_commits_per_day(commits_by_date):
    # Create commits_per_day_df to hold the count of the number of commits per day by repo_name:
    commits_per_day = commits_by_date.groupby(
        ['repo_name', 'author_date']).size()
    commits_per_day_df = commits_per_day.reset_index(name='number_of_commits')
    commits_per_day_df = commits_per_day_df.set_index(
        ['repo_name', 'author_date'])

    return commits_per_day_df


def compute_day_analysis(commits_by_date):
    # Which days had the most commits:
    day_analysis = commits_by_date.groupby(['repo_name', 'day_of_the_week']).agg({
        'day_of_the_week': 'count'
    }).rename(columns={'day_of_the_week': 'number_of_commits_on_day'}).reset_index()

    return day_analysis


def compute_commit_msg_analysis(commits_by_date):
    # Message Length analysis:
    commit_msg_analysis = commits_by_date.copy()
    commit_msg_analysis = commit_msg_analysis.groupby(['repo_name']).agg({
        'message_length': ['mean', 'median']
    })

    # Flatten new columns:
    commit_msg_analysis.columns = [
        f'{top_level}_{bottom_level}' for top_level, bottom_level in commit_msg_analysis.columns]

    commit_msg_analysis = commit_msg_analysis.rename(columns={
        'message_length_mean': 'avg_message_length',
        'message_length_median': 'median_message_length'
    }).reset_index()

    return commit_msg_analysis


def compute_final_summary(issues_summary, commits_summary, info_summary, closed_issues_summary):

    summary_df = pd.merge(issues_summary, commits_summary,
                          on='repo_name', how='inner')
    summary_df = pd.merge(summary_df, info_summary,
                          on='repo_name', how='inner')
    summary_df = pd.merge(summary_df, closed_issues_summary,
                          on='repo_name', how='inner')

    return summary_df


# define save methd:

def save_cleaned_data(dataframe, filename):
    """Save data to csv file in the data/clean directory"""
    clean_data_dir = Path('../data/cleaned')
    clean_data_dir.mkdir(parents=True, exist_ok=True)

    filepath = clean_data_dir/filename

    dataframe.to_csv(filepath, index=False)

    print(f"Saved to {filepath}")
