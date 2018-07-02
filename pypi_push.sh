# first bump up package.json manually, commit and tag
rm airflow/www_rbac/static/dist/*
cd airflow/www_rbac/static/
yarn
npm run build
cd ../../..
python setup.py sdist upload

