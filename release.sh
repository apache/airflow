#!/bin/bash
#
# Bump the version, run auto-changelog, and push to Git
#
# Based on:
# - https://gist.github.com/pete-otaqui/4188238
# - https://gist.github.com/mareksuscak/1f206fbc3bb9d97dec9c
#
# Requirements:
# - `npm i -g auto-changelog`
# - `pip3 install wheel twine`

# define the file containing the version here
VERSION_FILE="airflow/version.py"

# run checks
command -v auto-changelog >/dev/null 2>&1 || { echo >&2 "auto-changelog is not installed. Install via npm!"; exit 1; }
python -c "import pypandoc" || { echo >&2 "pypandoc is not installed. Install via pip!"; exit 1; }
python -c "import twine" || { echo >&2 "twine is not installed. Install via pip!"; exit 1; }
python -c "import wheel" || { echo >&2 "wheel is not installed. Install via pip!"; exit 1; }
[[ -z $(git status -s) ]] || { echo >&2 "repo is not clean, commit everything first!"; exit 1; }

set -e

NOW="$(date +'%B %d, %Y')"
RED="\033[1;31m"
GREEN="\033[0;32m"
YELLOW="\033[1;33m"
BLUE="\033[1;34m"
PURPLE="\033[1;35m"
CYAN="\033[1;36m"
WHITE="\033[1;37m"
RESET="\033[0m"
QUESTION_FLAG="${GREEN}?"
WARNING_FLAG="${YELLOW}!"
NOTICE_FLAG="${CYAN}❯"

PUSHING_MSG="${NOTICE_FLAG} Pushing new version to the ${WHITE}origin${CYAN}..."

LATEST_HASH="$(git log --pretty=format:'%h' -n 1)"
# get the semver version
BASE_STRING=$(grep -Eo 'c[0-9]+\.[0-9]+\.[0-9]+' "$VERSION_FILE")
BASE_LIST=(`echo $BASE_STRING | tr '.' ' '`)
V_MAJOR=${BASE_LIST[0]}
V_MINOR=${BASE_LIST[1]}
V_PATCH=${BASE_LIST[2]}
echo -e "${NOTICE_FLAG} Current version: ${WHITE}$BASE_STRING"
echo -e "${NOTICE_FLAG} Latest commit hash: ${WHITE}$LATEST_HASH"
V_MINOR=$((V_MINOR + 1))
V_PATCH=0
SUGGESTED_VERSION="$V_MAJOR.$V_MINOR.$V_PATCH"
echo -ne "${QUESTION_FLAG} ${CYAN}Enter a version number [${WHITE}$SUGGESTED_VERSION${CYAN}]: "
read INPUT_STRING
if [ "$INPUT_STRING" = "" ]; then
    INPUT_STRING=$SUGGESTED_VERSION
fi
echo -e "${NOTICE_FLAG} Will set new version to be ${WHITE}$INPUT_STRING"

# replace the python version
perl -pi -e "s/\Q$BASE_STRING\E/$INPUT_STRING/" "$VERSION_FILE"

# add the change itself
git add "$VERSION_FILE"

# bump initially but to not push yet
git commit -m "Bump version to ${INPUT_STRING}."
git tag -a -m "Tag version ${INPUT_STRING}." "v$INPUT_STRING"

# generate the changelog
auto-changelog -v ${INPUT_STRING}

# add the changelog and amend it to the previous commit and tag
git add CHANGELOG.md
git commit --amend --no-edit
git tag -a -f -m "Tag version ${INPUT_STRING}." "v$INPUT_STRING"

# push to remote
#echo -e "$PUSHING_MSG"
#git push && git push --tags

# upload to PyPi
#rm -rf dist/* build
#python3 setup.py sdist bdist_wheel
#python3 -m twine upload dist/*
