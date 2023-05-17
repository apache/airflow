# Hello! It is a tool that can be run daily or weekly to analyze information about the open source community. It requests information from the Git API and generates a report on the latest state of the community.

# Installation 
git clone git@github.com/AibekYrysbekov/airflow

# Create virtualenv venv and install requirements.txt
pip3 install -r requirements.txt


# create file .env and write 

ALLOWED_HOSTS=localhost,127.0.0.1
DEBUG=True
SECRET_KEY=django-insecure-g%@=fqg@*a_o%$c()w=-yy@iv@plz2jij8*46gw_2jy1&z(&2=
TOKEN=<your token>

# Run server 
python3 manage.py runserver
