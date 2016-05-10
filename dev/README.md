# Development Tools

## Airflow Pull Request Tool

The `airflow-pr` tool interactively guides committers through the process of merging GitHub PRs into Airflow and closing associated JIRA issues.

It is very important that PRs reference a JIRA issue. The preferred way to do that is for the PR title to begin with [AIRFLOW-XX]. However, the PR tool can recognize and parse many other JIRA issue formats in the title and will offer to correct them if possible.

__Please note:__ this tool will restore your current branch when it finishes, but you will lose any uncommitted changes. Make sure you commit any changes you wish to keep before proceeding.

Also, do not run this tool from inside the `dev` folder if you are working with a PR that predates the `dev` directory. It will be unable to restore itself from a nonexistent location. Run it from the main airflow directory instead: `dev/airflow-pr`.

### Execution
Simply execute the `airflow-pr` tool:
```
$ ./airflow-pr
Usage: airflow-pr [OPTIONS] COMMAND [ARGS]...

  This tool should be used by Airflow committers to test PRs, merge them
  into the master branch, and close related JIRA issues.

  NOTE: this tool will restore your current branch when it finishes, but
  you will lose any uncommitted changes.

  *** Please commit any changes you wish to keep before proceeding. ***

Options:
  --help  Show this message and exit.

Commands:
  merge       Merge a GitHub PR into Airflow master
  work_local  Clone a GitHub PR locally for testing (no push)
```

#### Commands

Execute `airflow-pr merge` to be interactively guided through the process of merging a PR, pushing changes to master, and closing JIRA issues.

Execute `airflow-pr work_local` to only merge the PR locally. The tool will pause once the merge is complete, allowing the user to explore the PR, and then will delete the merge and restore the original development environment.

Both commands can be followed by a PR number (`airflow-pr merge 42`); otherwise the tool will prompt for one.


### Configuration

#### Python Libraries
The merge tool requires the `click` and `jira` libraries to be installed. If the libraries are not found, the user will be prompted to install them:
```bash
pip install click jira
```

#### git Remotes
Before using the merge tool, users need to make sure their git remotes are configured. By default, the tool assumes a setup like the one below, where the github repo remote is named `github` and the Apache repo remote is named `apache`. If users have other remote names, they can be supplied by setting environment variables `GITHUB_REMOTE_NAME` and `APACHE_REMOTE_NAME`, respectively.

```bash
$ git remote -v
apache	https://git-wip-us.apache.org/repos/asf/incubator-airflow.git (fetch)
apache	https://git-wip-us.apache.org/repos/asf/incubator-airflow.git (push)
github	https://github.com/apache/incubator-airflow.git (fetch)
github	https://github.com/apache/incubator-airflow.git (push)
origin	https://github.com/<USER>/airflow (fetch)
origin	https://github.com/<USER>/airflow (push)
```

#### JIRA
Users should set environment variables `JIRA_USERNAME` and `JIRA_PASSWORD` corresponding to their ASF JIRA login. This will allow the tool to automatically close issues.

#### GitHub OAuth Token
Unauthenticated users can only make 60 requests/hour to the Github API. If you get an error about exceeding the rate, you will need to set a `GITHUB_OAUTH_KEY` environment variable that contains a token value. Users can generate tokens from their GitHub profile.
