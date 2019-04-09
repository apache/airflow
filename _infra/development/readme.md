## Dev model

- git branch + remote origin set
- airlab sync
- docker image

### 1. git branch + remote origin

1. the branch `rb1.10.0` is under remote `origin`
2. the master branch is to track apache master. You want to make sure it is set
   to remote `apache` by running
   `git remote add apache git@github.com:apache/airflow.git`
3. Now if you do `git remote -v`:

```
~/airlab/repos/airflow-internal(rb1.10.0)*
$ git remote -v
apache	git@github.com:apache/airflow.git (fetch)
apache	git@github.com:apache/airflow.git (push)
origin	git@git.musta.ch:airbnb/airflow-internal.git (fetch)
origin	git@git.musta.ch:airbnb/airflow-internal.git (push)
```
4. When you are in the master branch: `git pull apache master`. When you are
   in `rb1.10.0`, run `git pull origin rb1.10.0` to update your local code

### 2. lab sync

`lab sync` will sync all directories under `~/airlab/repos/` and it also
watches any file changes and sync to remote saturn5

### 3. docker image

The docker image is to make sure all the airflow dependencies and runtime are same across
all machines. There are some important points:

- file mount.
    - the `AIRFLOW_HOME` is mounted to `~/airlab/repos/airflow_remote_dev/apache/` or `~/airlab/repos/airflow_remote_dev/internal.1.10.0/` based on the docker image name, which means you can change the settings in your local
      https://git.musta.ch/ping-zhang/airflow_remote_dev

    - airflow src code is also mounted and I am using `pip install --no-deps -q -e file:///airflow/#egg=apache-airflow`, which allows you to change your local code without reinstalling it
    the flow is: 1) you change your local airflow src 2) lab sync the change to saturn5 3) as the file mount of airflow src and the `pip -e`, the changes outside the docker will automically reflected

- port mapping
    - there is a port mapping from docker image to saturn5 and then lab sync
      also has a traffic tunnel from saturn5 to your local mac so that you can
      visit `localhost:8080` or `localhost:8081`

## How to:

## Setup once:
```
ln -s /usr/local/opt/readline/lib/libreadline.8.dylib /usr/local/opt/readline/lib/libreadline.7.dylib

cd ~/airlab/repos

# the airflow_remote_dev has airflow.cfg and user dags/
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

## after you are on your saturn5 host, start to dev


#### 1. create a docker image
```
cd ~/repos/airflow-internal
./_infra/development/local_dev.sh <docker_image_tag> <docker_image>
```
**NOTE: if this is the first time that you run this, it will take a while to build the docker image**

`docker_image` is used to tell the script to use which Dockerfile to create the
image, currently there are two: `Dockerfile.apache` and `Dockerfile.internal.1.10.0` under `_infra/development/dockerfiles/`

The `docker_image` will be `apache` or `internal.1.10.0` so basically after
`Dockerfile.` is the name

The `web PORT` for `apache` is `8081`. So visit `localhost:8081` in your local
The `web PORT` for `internal.1.10.0` is `8080`. So visit `localhost:8080` in your local

#### 2. After you are in the docker, create tmux session as you will need to run multiple `airflow` commands

To create a new session:
```
tmux new-session -s airflow
```
Please refer to this cheat sheet: https://gist.github.com/MohamedAlaa/2961058

#### 3. happy coding in your IDE/VIM in your local mac :)


### FAQ:

#### 1. Default dev username and password for rbac UI

```
Username: admin
Password: changeisgood
```

#### 2. How to change the `airflow.cfg` or write dags

Since the `AIRFLOW_HOME` inside the docker is `/root/airflow`, which is mounted
to `~/airlab/repos/airflow_remote_dev`, so you can write dags or update `cfg`
under this dir

#### 3. Where is the meta data store?

All components are in the docker image, including `mysql`, `redis`. So after
you exit the docker, the meta data will be gone. However, your airflow cfg is
mounted in your local `~/airlab/repos/airflow_remote_dev`

#### 4. How can I exit my docker image and attach to it next time?

To detach the tty without exiting the shell, use the escape sequence `Ctrl-p Ctrl-q`

To attach to the docker process, 1) run `docker ps` to find the correct
`Container ID`, 2) run `docker attach <Container ID>`


#### 5. Error saying your port is in use

It could be another process is using the port. Most likely, you already have
a docker process running. You can check it with cmd: `docker ps`

#### 6. If you see `docker unable to prepare context` in your saturn5:

```
sudo apt install docker-ce
```

#### 7. If your unit test failed with "AssertionError: 'Log for testing.' not found"

Since `lab sync` will ignore log files, you need to manually create the missing log files on your saturn5.

