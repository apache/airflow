Make sure you have checked _all_ steps below.

### Jira

- [ ] My PR addresses the following [Airflow Jira](https://issues.apache.org/jira/browse/AIRFLOW/) issues and references them in the PR title. For example, "\[AIRFLOW-XXX\] My Airflow PR"
  - https://issues.apache.org/jira/browse/AIRFLOW-XXX
  - In case you are fixing a typo in the documentation you can prepend your commit with \[AIRFLOW-XXX\], code changes always need a Jira issue.
  - In case you are proposing a fundamental code change, you need to create an Airflow Improvement Proposal([AIP](https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Improvements+Proposals)).

### Description

- [ ] Here are some details about my PR, including screenshots of any UI changes:

### Tests

- [ ] My PR adds the following unit tests __OR__ does not need testing for this extremely good reason:

### Commits

- [ ] My commits all reference Jira issues in their subject lines, and I have squashed multiple commits if they address the same issue. In addition, my commits follow the guidelines from "[How to write a good git commit message](http://chris.beams.io/posts/git-commit/)":
  1. Subject is separated from body by a blank line
  1. Subject is limited to 50 characters (not including Jira issue reference)
  1. Subject does not end with a period
  1. Subject uses the imperative mood ("add", not "adding")
  1. Body wraps at 72 characters
  1. Body explains "what" and "why", not "how"

### Documentation

- [ ] In case of new functionality, my PR adds documentation that describes how to use it.
  - When adding new operators/hooks/sensors, the autoclass documentation generation needs to be added.
  - All the public functions and the classes in the PR contain docstrings that explain what it does
  - If you implement backwards incompatible changes, please leave a note in the [Updating.md](https://github.com/apache/airflow/blob/master/UPDATING.md) so we can assign it to a appropriate release

### Code Quality

- [ ] Passes `flake8`
