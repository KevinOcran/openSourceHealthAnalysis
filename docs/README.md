# [PROJECT TITLE: Open-Source Project Health Analysis]

## Overview

This project establishes a data-driven framework using the GitHub API and Python ETL to analyze the health and maintenance status across five major open-source repositories (Pandas, NumPy, Scikit-learn, Airflow, MLflow). It solves the crucial business problem of resource allocation and risk management by providing objective metrics like Issue Age and Contributor Diversity to help project maintainers quickly identify potential backlogs and "bus factor" risks. The final analysis delivers three actionable insights aimed at optimizing triage workflows and increasing community engagement to boost project velocity

## Key Features & Insights

* **Data Collection Pipeline:** Successfully collected issues, commits, and repository metadata from 5 major open-source projects via the GitHub API.
* **Core Metric Calculation:** Engineered key metrics like **Issue Age (days)** and **Contributor Diversity**.

* **Visualization:** Identified trends in commit cadence and issue resolution times.

* **Actionable Insight 1:** High Variability in Issue Resolution Times Indicates Potential Maintenance Bottlenecks: The average issue age across repos ranges from 7.18 days (airflow) to 15.28 days (scikit-learn), with medians showing similar spreads (5–10.5 days). Longer ages, especially in repos like scikit-learn and numpy, suggest slower triage or resolution, potentially leading to unresolved bugs and user dissatisfaction. Recommendation: Prioritize repos with higher median issue ages by implementing automated triage tools (e.g., GitHub bots) or dedicated maintainer rotations to reduce resolution time by 20–30%, improving user retention and project velocity.

* **Actionable Insight 2:** Limited Contributor Diversity Poses Risks to Long-Term Sustainability: Commit summaries reveal that while total commits are consistent (100 per repo in the sample), unique authors vary, with some repos (e.g., pandas and numpy) showing fewer distinct contributors (implying reliance on a small core team). This "bus factor" risk could halt progress if key individuals leave. Recommendation: Launch contributor outreach programs, such as hackathons or mentorship for "good first issues," to increase unique authors by 15–20% over the next quarter, diversifying maintenance efforts and enhancing project resilience.

* **Actionable Insight 3:** Weekday Commit Peaks Suggest Opportunities for Optimized Collaboration: The visualization of commits by day of the week shows higher activity mid-week (e.g., Tuesday–Thursday) across all repos, with weekends dropping off significantly. This pattern indicates focused professional contributions but potential underutilization of global, asynchronous collaboration. Recommendation: Encourage weekend-friendly tasks (e.g., documentation or low-risk reviews) through incentives like badges or community spotlights, aiming to balance activity and boost overall commit volume by 10–15% to foster more consistent maintenance.

## Technical Summary

* **Project Type:** End-to-end Python-based Exploratory Data Analysis (EDA).
* **Data Source:** GitHub API (Public Data).
* **Final Summary Metrics Table:**
| repo_name                   | number_of_issues | avg_issue_age_days | median_issue_age_days | commit_count | stargazers_sum | average_time_to_close_hours |
|-----------------------------|------------------|--------------------|-----------------------|--------------|----------------|----------------------------|
| apache/airflow              | 100              | 15.18              | 18.0                  | 100          | 43145          | 77.385149                  |
| mlflow/mlflow               | 100              | 16.74              | 11.0                  | 100          | 22892          | 91.657217                  |
| numpy/numpy                 | 100              | 17.13              | 8.0                   | 100          | 30782          | 98.301624                  |
| pandas-dev/pandas           | 100              | 18.10              | 18.0                  | 100          | 47063          | 146.507114                 |
| scikit-learn/scikit-learn   | 100              | 19.47              | 7.0                   | 100          | 63980          | 116.270159                 |

## Tech Stack

* Language: Python 3.11.13
* Libraries: Pandas, Requests, Matplotlib, Seaborn
* Tools: Jupyter Lab, VSCode, Git
* Deployment: [Local analysis only, or GitHub Pages for documentation]

## Reproducible Setup

This project requires Python 3.x, **pip**, and a **GitHub Personal Access Token (PAT)**.

```bash
# Step 1: Clone repo
git clone https://github.com/KevinOcran/openSourceHealthAnalysis
cd openSourceHealthAnalysis

# Step 2: Configure environment variables
# Create a .env file in the root directory and add your PAT:
# GITHUB_TOKEN="your_personal_access_token"

# Step 3: Install dependencies
pip install -r requirements.txt # (Ensure you create this file)

# Step 4: Run the data collection pipeline
python src/api_fetch.py

# Step 5: Run the wrangling and analysis
# Open the main wrangling first and then the analysis notebook to view the ETL, cleaning, and results.
jupyter lab notebooks/wrangling_and_cleaning.ipynb
jupyter lab notebooks/analysis.ipynb
```

## Architecture

1. Extract (E): Data Collection (src/api_fetch.py)

    Function: Handles connection to the GitHub API, manages pagination, checks rate limits, and extracts raw data (issues, commits, repo info) in JSON format.

    Output: Unaltered raw JSON files stored in the /data/raw directory.

2. Transform (T): Data Processing (src/data_processing.py and notebooks/wrangling_and_cleaning.ipynb)

    Function: These modular script performs the bulk of the cleaning and transformation. It flattens the nested JSON structures, converts date strings to datetime objects, handles missing values, and engineers the critical feature issue_age_days.

    Output: Three normalized, analysis-ready DataFrames saved as Cleaned CSVs (cleaned_issues.csv, cleaned_commits.csv, etc.) in the /data/cleaned directory.

3. Load & Analyze (L/A): Exploratory Data Analysis (notebooks/analysis.ipynb)

    Function: The Jupyter Notebook acts as the final Consumer of the clean data. It loads the CSVs from /data/cleaned, executes the EDA (descriptive statistics, grouping), performs Visualization, and articulates the final Actionable Insights and recommendations.

    Output: High-resolution Visualizations (/output/visuals) and the final Summary Metrics Table.

## Data Sources

* GitHub API: Used the Issues, Commits, and Repos endpoints. Data is public and governed by GitHub's API Terms of Service.

## Screenshots/Demo

### 📈 1. Activity Trends: Issues Opened vs. Closed

This line plot reveals the project's ability to keep up with incoming user demand over time.

![Issues Opened vs. Closed Trend](output/visuals/fig1_issue_activity.png)

### 📊 2. Project Health Comparison: Average Issue Age

This bar chart shows which projects have the fastest and slowest issue resolution times, highlighting a potential bottleneck in scikit-learn.

![Average Issue Age Comparison](output/visuals/fig2_avg_issue_age.png)

### 👥 3. Contributor Diversity: Top 10 Contributors by Repo

![Top 10 contributers in each repo](output/visuals/fig3_top_10_contributers_by_repo.png)

### 🕒 4. Issue Age Distribution Across Repositories

![Distribution of How Long Issues Take to Close](output/visuals/fig4_issue_age_distribution.png)

### 📉 5. Log Transformed Issue Age Distribution

![Log Transformed graph of the Issue Age Distribution](output/visuals/fig5_log_issue_age_distribution.png)

### 📅 6. Commit Activity by Day of the Week

![Distribution of Commits Per Day of the Week](output/visuals/fig6_commit_distribution_across_repo.png)
***

## Known Limitations

1. Data Volume Constraint: "Only the 100 most recent issues and commits were collected per repository due to GitHub API rate limiting constraints, limiting the scope of historical trend analysis to recent activity."

2. Scope Limitation: "Analysis focused exclusively on core metrics (issues, commits, stars). Future work could integrate external data (e.g., Slack activity, PR review times) or utilize more advanced natural language processing (NLP) on issue text and commit messages."

3. Lack of Automation: "The pipeline is currently designed for single-run, local execution and lacks scheduling via tools like Airflow or a robust Docker environment for continuous, production-level data refreshing."

## License

[MIT, Apache 2.0, etc.]

## Contact

**email**: [ocrankevin42@gmail.com]

**LinkedIn:** [www.linkedin.com/in/kevin-atoampong-ocran]
