# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a fantasy baseball player performance prediction project that combines web scraping, SQL data processing, and custom machine learning models. The project predicts player performance using historical baseball statistics and real-time data (lineups, weather, stats).

## Architecture

### Code Organization

The codebase is structured into three main areas:

1. **Code/OOM/** - Object-oriented machine learning module
   - Base classes: `Regression` and `Classification` provide foundations for all models
   - Implemented models: `OLS`, `LogisticRegression`, `GLM`, `Poisson`, `NegativeBinomial`, `Exponential`, `MultinomialLogisticRegression`, `Tree`, `RandomForest`
   - The `Regression` class (Code/OOM/Regression.py:3) expects numpy arrays or polars DataFrames as input and provides common methods like `predict()`, `RMSE()`, `MSE()`, `MAE()`
   - The `Classification` class (Code/OOM/Classification.py:10) provides binary classification with methods like `predict()`, `predict_binary()`, `conf_matrix()`, `f_score()`, `auc_roc()`, `log_loss()`, `calibration()`, `SomersD()`
   - Models handle both numpy arrays and polars DataFrames through automatic conversion

2. **Code/Production/** - Production data pipeline classes
   - `Database` class (Code/Production/create_model_ready.py:12): MySQL database operations using sqlalchemy
     - `db_connect()`: Creates pandas/sqlalchemy engine
     - `db_connect_polars()`: Creates Polars-compatible URI
     - `db_pull()`: Returns polars DataFrames from SQL queries
     - `db_insert()`: Inserts pandas DataFrames into tables
   - `WebScrape` class (Code/Production/create_model_ready.py:74): Inherits from Database, scrapes real-time data
     - `get_weather(city)`: Scrapes Google weather data
     - `get_lineups(date)`: Scrapes baseballpress.com for daily lineups (format: 'YYYY-MM-DD')
     - `get_stats(date)`: Uses pybaseball package to fetch statcast data

3. **Code/Data Preparation/** - SQL scripts for feature engineering
   - Data_Processing_DV.sql: Main SQL processing script that creates at-bat level tables and calculates fantasy points using scoring system defined by variables (@single=3, @double=5, @hr=10, etc.)

### Data Flow

1. Raw baseball data stored in MySQL database (accessed via Database class)
2. SQL scripts (Data Preparation/) transform raw data into at-bat level features with fantasy point calculations
3. Python scripts scrape real-time data (lineups, weather, statcast) and insert into database
4. Processed data fed into OOM models for prediction
5. Models output predictions for fantasy baseball performance

### Key Dependencies

- **Data handling**: numpy, pandas, polars (dual support throughout)
- **Database**: sqlalchemy, pymysql
- **ML/Stats**: scikit-learn (used internally by Classification/LogisticRegression), scipy, statsmodels
- **Web scraping**: requests, beautifulsoup4, pybaseball
- **Visualization**: matplotlib

### Important Implementation Details

**DataFrame Handling**: All OOM models accept both numpy arrays and polars DataFrames. Models automatically convert polars to numpy internally (see Regression.py:8-13 and Classification.py:23-28).

**Database Connections**: Use `Database.db_connect_polars()` when working with polars, `db_connect()` for pandas/sqlalchemy operations.

**Team Code Mapping**: Team abbreviations follow Retrosheet format (e.g., "ANA" for Angels, "NYA" for Yankees). Conversion mappings in create_model_ready.py:174-205 (full names to codes) and 259-290 (statcast codes to Retrosheet).

**Model Inheritance**: Subclass `Regression` for continuous outcomes or `Classification` for binary outcomes. Call `super().__init__(x, y)` and implement `fit()` method. The base classes handle prediction and evaluation metrics.

**Date Formats**: Web scraping methods expect 'YYYY-MM-DD' format but internally convert to 'YYYYMMDD' for GAME_ID construction.

**Retrosheet Events Data**: The raw play-by-play data follows the Retrosheet event file format. See Resources/event_file.md for complete documentation on the events table structure, field definitions, and play-by-play coding system. Key points:
- GAME_ID is a 12-character identifier (e.g., "ATL198304080" = Atlanta, 1983-04-08, game 0)
- The events table does NOT contain a GAME_DATE column; dates must be extracted from GAME_ID using `STR_TO_DATE(SUBSTRING(GAME_ID, 4, 8), '%Y%m%d')`
- Player IDs are 8-character Retrosheet codes (first 4 letters of last name + first initial + 3-digit number)
- Event codes define play outcomes (e.g., EVENT_CD: 20=single, 21=double, 22=triple, 23=home run, 3=strikeout, 14=walk)

**Fantasy Scoring System**: Defined in Data_Processing_DV.sql:3-23 with specific point values for different baseball events (singles=3, HR=10, strikeouts=2, etc.).

## Testing

No formal test framework is configured. Test models manually using historical data from the database.

## Running Code

This project is primarily intended to be run interactively or via scripts that:
1. Instantiate Database/WebScrape classes with credentials
2. Pull data using `db_pull()` with SQL queries
3. Scrape real-time data with `get_lineups()`, `get_weather()`, `get_stats()`
4. Fit OOM models with the processed data
5. Generate predictions using model `predict()` methods

No build, lint, or automated test commands are configured.

## Git Usage

### Branch Management

**Main Branches**:
- `master`: Stable, tested configurations
- Feature branches: For development and testing

**Branch Operations**:
- Stay on current branch unless explicitly instructed to switch
- Create feature branches for experimental changes: `git checkout -b feature/description`
- Never delete branches without approval
- Keep branch names descriptive and lowercase with hyphens

### Commit Message Conventions

Follow conventional commits format:

```
<type>(<scope>): <short description>

[optional body]

[optional footer]
```

**Types**:
- `feat`: New feature or module
- `fix`: Bug fix or correction
- `refactor`: Code restructuring without behavior change
- `docs`: Documentation updates
- `style`: Formatting, code style changes
- `chore`: Maintenance tasks (dependency updates, cleanup)
- `test`: Adding or updating tests

**Examples**:
- `feat(models): add RandomForest classifier`
- `fix(sql): correct GAME_DATE column reference in data processing`
- `chore: update pandas to 2.0.0`
- `refactor(database): simplify db_pull method`

**Commit Message Rules**:
- Focus on WHAT changed and WHY, not HOW it was created
- Do NOT include references to Claude Code, AI generation, or automated tools
- Remove any footers like "Generated with Claude Code" or "Co-Authored-By: Claude"
- Keep messages professional and tool-agnostic
- Commit history should reflect project changes, not development methodology

### Commit Workflow

**Before Committing**:
1. Test changes with relevant data/queries
2. Review changes: `git diff`
3. Stage relevant files: `git add <files>`

**Creating Commits**:
- Commit logical units of work (one feature/fix per commit)
- Always wait for explicit user approval before committing
- Claude will propose 1-3 commit messages
- User must explicitly say "commit" or "yes, commit" to proceed

**Never Commit**:
- Broken code or SQL scripts
- Untested changes
- Secrets, credentials, or database connection strings
- Large data files (use .gitignore)

### Push/Pull Policy

**Pulling**:
- Pull before starting new work: `git pull origin master`
- Use rebase to keep history clean: `git pull --rebase`

**Pushing**:
- NEVER push without explicit user approval
- Always push to feature branches first
- Never force-push to `master`
- Force-push to feature branches only with approval: `git push --force-with-lease`

### Merge/Rebase Strategy

**For Feature Branches**:
- Rebase on master to keep history linear: `git rebase master`
- Squash commits if needed before merging: `git rebase -i master`

**For Master Branch**:
- Merge feature branches with: `git merge --no-ff feature/name`
- Keep merge commits for features
- Fast-forward for small fixes

**Never Without Approval**:
- Interactive rebase
- History rewriting (amend, reset, rebase) on pushed commits
- Merge to master branch

### Common Operations

**Status Check**:
```bash
git status              # Check working tree
git log --oneline -10   # View recent commits
git diff                # View unstaged changes
git diff --staged       # View staged changes
```

**Stashing**:
```bash
git stash               # Save work in progress
git stash pop           # Restore stashed changes
git stash list          # View all stashes
```

**Undoing Changes**:
```bash
git restore <file>      # Discard unstaged changes
git restore --staged <file>  # Unstage file
git reset HEAD~1        # Undo last commit (keep changes)
```

**Viewing History**:
```bash
git log --graph --oneline --all  # Visual branch history
git show <commit>                # Show specific commit
git blame <file>                 # See line-by-line authorship
```

### After Changes Checklist

When Claude makes changes:

1. ✅ Show `git status`
2. ✅ Show `git diff` for modified files
3. ✅ List all files changed
4. ✅ Report test results (if applicable)
5. ✅ Propose 1-3 commit messages following conventions
6. ⏸️ **WAIT** for explicit commit approval
7. ⏸️ **WAIT** for explicit push approval (if applicable)

### Emergency Procedures

**If Code Breaks**:
1. Don't commit
2. Review git diff to identify issues
3. Restore last working state if needed: `git restore .`

**If Accidentally Staged**:
```bash
git restore --staged .  # Unstage everything
```

**If Need to Abort Merge**:
```bash
git merge --abort
```
