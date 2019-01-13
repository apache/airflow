# INSTALL / BUILD instructions for Apache Airflow

# [required] fetch the tarball and untar the source
# change into the directory that was untarred.

# [optional] run Apache RAT (release audit tool) to validate license headers
# RAT docs here: https://creadur.apache.org/rat/. Requires Java and Apache Rat
java -jar apache-rat.jar -E ./.rat-excludes -d .

# [optional] Airflow pulls in quite a lot of dependencies in order
# to connect to other services. You might want to test or run Airflow
# from a virtual env to make sure those dependencies are separated
# from your system wide versions
python -m my_env
source my_env/bin/activate

# [required] building and installing
# by pip (preferred)
pip install .

# or directly
python setup.py install
