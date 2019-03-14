## How to:


## Setup once:
```
ln -s /usr/local/opt/readline/lib/libreadline.8.dylib /usr/local/opt/readline/lib/libreadline.7.dylib

cd ~/airlab/repos

git clone git@git.musta.ch:ping-zhang/airflow_remote_dev.git ~/airlab/repos/airflow_remote_dev
git clone git@git.musta.ch:airbnb/airflow-internal.git

cd airflow-internal
git checkout rb1.10.0

# if you have a new saturn5 host:

lab config saturn5.instance_hostname <your saturn5 hostname> # find it: https://starforge.d.musta.ch/saturn5
lab provision # if it fails, run it multiple times

lab enable airflow-cluster # if it fails, it is fine
lab provision # if it fails, it is fine
```

## When you start to develop:

### on your local terminal:
```
lab sync
```

## ssh into your saturn5 in another window

```
lab ssh
```

## after you are on your saturn5 host

```
cd ~/repos/airflow-internal
./_infra/development/local_dev.sh <docker_image_tag> <docker image>
```

`docker image` is used to tell the script to use which Dockerfile to create the
image, currently there are two:
Dockerfile.apache  Dockerfile.internal.1.10.0

The `PORT` for `apache` is `8081`
The `PORT` for `internal.1.10.0` is `8080`

The `docker image` will be `apache` or `internal.1.10.0` so basically after
`Dockerfile.` is the name

# if this is the first time that you run this, it will take a while to build the docker image

After you are in the docker, it is better to create tmux sessions to run
`airflow` commands


## FAQ:

- How to change the `airflow.cfg` or write dags

Since the `AIRFLOW_HOME` inside the docker is `/root/airflow`, which is mounted
to `~/airlab/repos/airflow_remote_dev`, so you can write dags or update `cfg`
under this dir


- If you see `docker unable to prepare context` in your saturn5:

```
sudo apt install docker-ce
```
