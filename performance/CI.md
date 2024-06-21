# Continuous Integration

The CI environment uses Docker image heavily. Currently, it is used in Cloud Build,
but you can adapt it easily to any CI. Follow the [cloudbuild.yaml](cloudbuild.yaml) as an example.

## The CI image

The CI image runs all the CI tests using Cloud Build. It is designed in the way that other CI
runners should be possible to adapt easily to use the same image.

It is used to run pytest and pre-commit runs.

You can build and push the image in a single step:
```shell script
./scripts/ci-image/build_and_push.sh
```

It should create image in Google Container Registry assigned to your project:

  - "gcr.io/${PROJECT_ID}/airflow-gepard-ci-build-image:latest"


You can also build and push the image separately:

Build:
```shell script
./scripts/ci-image/build.sh
```

Push:
```shell script
./scripts/ci-image/push.sh
```

## Running CI tests locally

You can run all cloud build process locally following the
[Building and debugging locally](https://cloud.google.com/cloud-build/docs/build-debug-locally) guide.

You can run the images manually as well. You just need to mount local sources to corresponding folder
inside the container and run appropriate commands. The general pattern to follow (running it from
the ROOT of your project sources):

```shell script
docker run -v $(pwd):$(pwd) -w $(pwd) ${CONTAINER_REGISTRY_URL}/airflow-gepard-ci-build-image:latest <CMD>
```

For example, to run pytest tests when your image uses GCR Registry:

```shell script
export PROJECT_ID=$(gcloud config get-value core/project)
docker run -v $(pwd):$(pwd) -w $(pwd)/sfdc-airflow-aas \
  gcr.io/${PROJECT_ID}/airflow-gepard-ci-build-image:latest \
  pytest tests/ -vvv --color=yes
```

To run pre-commit tests in similar case. Pre-commits require additional docker socket to be
mounted to inside the container as some pre-commits use docker internally:

```shell script
export PROJECT_ID=$(gcloud config get-value core/project)
docker run -v $(pwd):$(pwd) -w $(pwd) \
  -v /var/run/docker.sock:/var/run/docker.sock \
  gcr.io/${PROJECT_ID}/airflow-gepard-ci-build-image:latest \
  bash ./scripts/run_precommit.sh
```

**NOTE!!**

If you are using Linux and run the scripts manual or via local cloud build debugging
the pre-commit script might change the ownership of some sources to root user, and you might
have problems with building the images
due to permission problems. In this case you should run the
```shell script
./scripts/ci-image/fix_ownership.sh
```

script to fix the ownership for those files.


## CI optimizations

Below CI Optimisations of Cloud Build can also be similarly implemented by any CI system.

* The docker image created is stored in the Container registry
  specified by the `CONTAINER_REGISTRY_URL` variable. The push step in CI process stores the
  image in the registry with the `latest` tag with
  the `IMAGE_TAG` for the CI images. Those images are the cache source to build the following
  images so once you have an image in the cache, subsequent rebuilds will be much faster.

* Pre-commit cache can be saved and restored between the builds. It takes several minutes to
  initialize the pre-commit cache, so saving and restoring it, is well-worth the effort.
  The scripts to cache these are in the `scripts/cache` folder, and you can use them
  similarly to how they are used in Cloud Build in any CI system.

  You can also configure the location where the script caches the files by setting
  `CACHE_URL_PREFIX` variable (for example `gs://your-cache-bucket/`). Your service
  account running the CI build should have appropriate permissions to read/write to the storage.
  As an example, you can read about permissions needed by Cloud Build in the
  [Optimizing Build Speed Documentation](https://cloud.google.com/cloud-build/docs/speeding-up-builds#caching_directories_with_google_cloud_storage).

  You can also change the utility to copy cache file back/forth (for example, it can be
  `gsutil` or `s3` for example). You can do it by setting the `CACHE_UTILITY` variable.
  The cache is immutable, so once it is stored, and the pre-commit related files
  do not change, it is re-used across the builds rather than replaced.

  For example, to run pre-commit tests with restoring cache before and storing cache
  after pre-commit, you can run it as follows:

    ```shell script
    export PROJECT_ID=$(gcloud config get-value core/project)

    docker run -it -v /tmp/cache:/cache -v /tmp/.cache:/root/.cache \
        -v $(pwd):$(pwd) -w $(pwd) \
        -v ${HOME}/.boto:/root/.boto -v ${HOME}/.config:/root/.config \
        --entrypoint bash gcr.io/cloud-builders/gsutil ./scripts/cache/restore_cache.sh

    docker run -it -v /tmp/cache:/cache -v /tmp/.cache:/root/.cache \
        -v $(pwd):$(pwd) -w $(pwd) \
        -v /var/run/docker.sock:/var/run/docker.sock \
        gcr.io/${PROJECT_ID}/airflow-gepard-ci-build-image:latest ./scripts/run_precommit.sh

    docker run -it -v /tmp/cache:/cache -v /tmp/.cache:/root/.cache \
        -v $(pwd):$(pwd) -w $(pwd) \
        -v ${HOME}/.boto:/root/.boto -v ${HOME}/.config:/root/.config \
        -w $(pwd) --entrypoint bash gcr.io/cloud-builders/gsutil ./scripts/cache/store_cache.sh

    ```

  It is important though that all three steps (`restore_cache`, `pre-commit` and `store_cache` share
  common volumes as `/cache` and `${HOME}/.cache` folders. In case of Cloud Build this is achieved
  by defining the volumes for all steps and setting the ${HOME} to /root - this way
  cache can be shared between local runs (see above) and the Cloud Build runs (home should
  be set to /root so that it can work):

  ```yaml
   env:
     - 'HOME=/root'
   volumes:
     - name: 'cache-tmp-dir'
       path: /cache
     - name: 'pre-commit-cache-dir'
       path: /root/.cache
  ```

## CI Triggers

When you set up your CI, There should be two triggers defined: Pull Request (to run CI builds
in case of incoming Pull Requests) and Push (to run CI after the Pull Request has been merged).
In the current repository, the main branch is `master` so both should target it.
In case of Cloud Build, those triggers should be defined with `^mastter$` branch name regular
expression.
