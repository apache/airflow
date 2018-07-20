Make sure you have checked _all_ steps below.

### JIRA
- [ ] My PR addresses the following [Airflow JIRA](https://issues.apache.org/jira/browse/AIRFLOW/) issues and references them in the PR title. For example, "\[AIRFLOW-XXX\] My Airflow PR"
    - https://issues.apache.org/jira/browse/AIRFLOW-XXX
    - In case you are fixing a typo in the documentation you can prepend your commit with \[AIRFLOW-XXX\], code changes always need a JIRA issue.


### Description
- [ ] Here are some details about my PR, including screenshots of any UI changes:


### Tests
- [ ] My PR adds the following unit tests __OR__ does not need testing for this extremely good reason:


### Commits
- [ ] My commits all reference JIRA issues in their subject lines, and I have squashed multiple commits if they address the same issue. In addition, my commits follow the guidelines from "[How to write a good git commit message](http://chris.beams.io/posts/git-commit/)":
    1. Subject is separated from body by a blank line
    2. Subject is limited to 50 characters
    3. Subject does not end with a period
    4. Subject uses the imperative mood ("add", not "adding")
    5. Body wraps at 72 characters
    6. Body explains "what" and "why", not "how"


### Documentation
- [ ] In case of new functionality, my PR adds documentation that describes how to use it.
    - When adding new operators/hooks/sensors, the autoclass documentation generation needs to be added.


### Code Quality
- [ ] Passes `git diff upstream/master -u -- "*.py" | flake8 --diff`
