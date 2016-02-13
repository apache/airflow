set -o verbose

python setup.py test --tox-args="-v -e $TOX_ENV"
