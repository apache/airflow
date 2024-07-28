# Airflow performance tests

## Requirements

Requirements differ depending on type of environment you want to run the tests on.

### Cloud Composer environment

[Google Cloud Composer](https://cloud.google.com/composer) is a fully managed workflow orchestration
service built on Apache Airflow.

#### Base requirements

1.  Cloud project with an enabled billing
1.  Cloud Composer API enabled
1.  Your service account (the one you authenticate to Cloud with) must have the following
    permissions: - composer.environments.create - to create Composer instance - composer.environments.delete - to delete Composer instance after tests finish - composer.environments.get - to get details about created Composer instance (GKE cluster etc.) - composer.environments.list - to check if your Composer instance already exists - composer.environments.update - to update Composer instance (new elastic DAG configuration) - container.clusters.get - container.clusters.list - to authenticate to the GKE cluster of your Composer instance - container.namespaces.get - container.namespaces.list - to find the namespace of airflow-worker pods - container.pods.exec - to execute python code on pods - container.pods.get - container.pods.list - to find an airflow-worker pod on your GKE cluster - iam.serviceAccounts.actAs - in order to be able to act on behalf of another service account
    (the one used by the Composer environment itself) - monitoring.timeSeries.list - to collect time series data from Cloud Monitoring - storage.buckets.get - to retrieve the bucket where DAG files are stored - storage.objects.create - to upload DAG files to Composer instance's storage. - storage.objects.delete - needed to overwrite existing DAG file with the same name (when
    reusing an existing environment)

        Roles that should suffice to grant the aforementioned permissions:
        - roles/composer.environmentAndStorageObjectAdmin
        (includes all composer.* and storage.objects.* permissions)
        - roles/storage.admin (for storage.buckets.* permissions)
        - roles/container.developer
        - roles/monitoring.viewer
        - roles/iam.serviceAccountUser (it suffices that you grant this role on the service account
        used by the Composer environment)

        Note that instead of a service account the script can use gcloud SDK's application default
        credentials (if they have been set).

1.  You need to have [gcloud SDK installed](https://cloud.google.com/sdk/install) - in order to
    authenticate to GKE cluster (note that private gcloud commands are used to do so).
    The same account mentioned above will be used for authentication with gcloud, you do not
    need to change the active account in gcloud yourself.
1.  The service account used by the Composer environment you create
    (by default: Compute Engine default service account, but you can specify your own in configuration
    file) must have the permissions of the `composer.worker` role.
1.  If you want to test an environment with some beta features,
    make sure beta feature support is enabled.

For more information on creating Composer environment,
see [Creating environments](https://cloud.google.com/composer/docs/how-to/managing/creating).

#### Routing requirements

If you intend to test a Composer environment that is behind a private IP you might additionally need
to enable routing connections via one of the GKE cluster's nodes.
This is needed if your private environment:

- has disabled access to the public master endpoint
- even though has the public master endpoint enabled, you are still unable to connect to it
  directly from the machine you are executing the scripts.

If you fall into one of the categories above, then you will have to make
some additional preparations:

1.  The service account used to run the script will need additional permissions:

    - compute.instances.get - in order to connect to node via SSH
    - compute.instances.setMetadata - to prepare node for connection (upload SSH key etc.)
    - compute.instanceGroups.get
    - compute.instanceGroups.list - in order to find a node belonging to private cluster
    - iap.tunnelInstances.accessViaIAP - in order to connect to the private node via
      Identity-Aware Proxy

    These permissions can be granted by following roles:

    - roles/compute.instanceAdmin
    - roles/iap.tunnelResourceAccessor

1.  You need to install `nodejs` in version greater than 4. You can find downloadable packages on
    [nodejs Downloads page](https://nodejs.org/en/download/). Once you have `nodejs`, you need to
    install [http-proxy-to-socks](https://github.com/oyyd/http-proxy-to-socks) tool by running commands:

        ```
        cd $HOME
        npm install http-proxy-to-socks
        ```

        Note that this will create `node_modules` directory and `/package-lock.json` file in your home
        folder. You must install the tool in your home folder for `npx` to work from any location.

1.  You need to [create a firewall rule](https://cloud.google.com/iap/docs/using-tcp-forwarding#create-firewall-rule)
    in the network that you intend to use that will allow IAP to make SSH connections
    to your cluster's nodes. For increased security you may want to enable this rule only
    for certain service accounts or virtual machines with specific tags assigned.

        Note that `default-allow-ssh` rule, that is by default enabled for the `default` network
        in your project, allows SSH connections from all IP addresses, not only from IAP, so it is
        possible that you can skip this step, if that is the network you are going to use.

Routing is never done for public IP Composer environments.

### Vanilla GKE environment

[Vanilla GKE environment](environments/kubernetes/gke/vanilla/vanilla_gke_environment.py)
is an environment installed directly on a GKE cluster using
[the official Apache Airflow helm chart](https://github.com/apache/airflow/tree/master/chart).
Current status:

- no support for reusing an existing environment
- no support for private cluster environments
- Airflow 2.0 environments currently experience issue with tasks hanging indefinitely in queued state
- Airflow 1.10.x environments will fail to create without [a fix from community](https://github.com/apache/airflow/pull/13526)

#### Base requirements

1.  Cloud project with an enabled billing
1.  Kubernetes Engine API and Container Registry API enabled
1.  Your service account (the one you authenticate to Cloud with) must have the following
    permissions: - compute.instanceGroups.get - compute.instanceGroups.list - container.clusters.create - to create GKE cluster - container.clusters.delete - to delete GKE cluster after tests finish - container.clusters.get - to get details about created GKE cluster - container.clusters.list - to check if GKE cluster with given name already exists - container.namespaces.create - to prepare namespace for airflow deployment - container.namespaces.get - container.namespaces.list - to find the namespace of airflow-worker pods - container.pods.exec - to execute python code on pods - container.pods.get - container.pods.list - to find an airflow-worker pod on your GKE cluster - container.roles.create - iam.serviceAccounts.actAs - in order to be able to act on behalf of another service account
    (the one used by the GKE cluster itself) - monitoring.timeSeries.list - to collect time series data from Cloud Monitoring

        Roles that should suffice to grant the aforementioned permissions:
        - roles/compute.instanceAdmin
        - roles/container.admin
        - roles/monitoring.viewer
        - roles/iam.serviceAccountUser (it suffices that you grant this role on the service account
        used by the Composer environment)

        Note that instead of a service account the script can use gcloud SDK's application default
        credentials (if they have been set).

1.  You need to have [gcloud SDK installed](https://cloud.google.com/sdk/install) - in order to
    authenticate to GKE cluster (note that private gcloud commands are used to do so).
    The same account mentioned above will be used for authentication with gcloud, you do not
    need to change the active account in gcloud yourself.
1.  [Docker installed](https://docs.docker.com/get-docker/) and
    [configured with gcloud credential helper](https://cloud.google.com/container-registry/docs/advanced-authentication#gcloud-helper).
1.  [Helm 2.11+ or Helm 3.0+](https://helm.sh/docs/intro/install/)

#### Routing requirements

Requirements for using routing with Vanilla GKE environment are the same as for Composer environments.

### Storing results

Airflow-gepard supports a few ways to store results, but in order to use some of them,
you need to fulfill a few requirements regardless of the environment type you run the tests on.

#### GCS bucket

To store results in a GCS bucket the service account you authenticate to Google Cloud project with
must have following permissions:

- storage.buckets.get - to retrieve the bucket where you want results to be stored
- storage.buckets.create - to create the results bucket (only in case it does not exist)
- storage.objects.create - to upload CSV files with results to the bucket.
- storage.objects.delete - to overwrite existing CSV file with results (needed only when the
  script is run reusing the same environment without resetting it)

roles/storage.admin is a role that should grant all the above permissions.

## Apache Airflow subrepo

Some environment types require access to the Apache Airflow code, for example the helm chart.
`airflow-gepard` uses [git-subrepo](https://github.com/ingydotnet/git-subrepo) to have
an easy access to the official repo with the ability to pull newest changes and contribute fixes
back to the upstream. If you do not intend to make any changes in the code, then you do not even
need to install `git-subrepo` and your regular workflow remains unchanged.
You only need to use subrepo commands when:

- you want to pull the latest changes from the linked repository
- you want to contribute the changes back to the linked repository

### Installing git-subrepo

To install `git-subrepo` run the following commands:

```
git clone https://github.com/ingydotnet/git-subrepo ${PATH_TO_SUBREPO}
echo 'source ${PATH_TO_SUBREPO}/.rc' >> ~/.bashrc
```

where `${PATH_TO_SUBREPO}` is the path to directory where you want to have `git-subrepo` installed.

### Subrepo setup

In `airflow-gepard`, Apache Airflow project is pulled into airflow-subrepo directory.
We use master branch of [the official Apache Airflow repository](https://github.com/apache/airflow)
and the strategy for subrepo pull commands is `merge`.

### Pulling latest changes from Apache Airflow

Running the following command:

`git subrepo pull airflow-subrepo`

will pull latest changes from master branch of PolideaInternal/airflow upstream repository, merge it
with the changes introduced in `airflow-gepard` and finally commit the result as a single commit.

Note, that executing subrepo pull command will change the commit referenced as a parent in
airflow-subrepo/.gitrepo configuration file, so that subsequent pulls can incrementally pull only
the new changes. This might cause issues if subrepo pulls are combined with other commits,
and parent commit is eventually rebased/squashed as a result of usual PR lifecycle. If that happens,
you will have to manually change the parent commit in the airflow-subrepo/.gitrepo before
the next subrepo pull.

### Pushing changes back to fork of upstream repo

Following the guidelines for [contributing to the Apache Airflow project](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst#contribution-workflow),
we do not push to the official Apache Airflow repository but to [our internal fork](https://github.com/PolideaInternal/airflow) instead.

To set up the `airflow-fork` remote pointing to this internal fork, use one of the commands
(you have to use it only once):

- `git remote add airflow-fork git@github.com:PolideaInternal/airflow.git` if you use ssh
- `git remote add airflow-fork https://github.com/PolideaInternal/airflow.git` if you use https.

Once you have the remote, running the following command:

`git subrepo push airflow-subrepo -r airflow-fork -b ${BRANCH_NAME}`

will push the local changes to the internal fork under ${BRANCH_NAME} branch. Then, you can create
PR between fork and official apache/airflow repository. Eventually, after this PR is merged into
the official repo, you will be able to synchronize airflow-gepard with it in the next subrepo pull.

### Reference

For more information on using `git-subrepo` refer to:

- [GitHub home of subrepo](https://github.com/ingydotnet/git-subrepo)
- [Simple and nice subrepo tutorial](http://blog.s-schoener.com/2019-04-20-git-subrepo/)
- [Wiki of subrepo project with basic information](https://github.com/ingydotnet/git-subrepo/wiki/Basics)

## Usage

### Virtual environment installation

To run the scripts you need virtual environment with at least python3.6.

Create new environment with any tool of your choosing (pyenv-virtualenv, virtualenvwrapper), then
activate it and install airflow_gepard package by running: `pip install -e .`
from the airflow-gepard directory.

If you plan to generate `.pdf` reports you need to install `wkhtmltopdf` binary:

```
sudo apt-get install wkhtmltopdf
# or on MacOs
brew install homebrew/cask/wkhtmltopdf
```

Additionally, if you plan on contributing to airflow_gepard, you should install pre-commit hooks
by running `pre-commit install` from airflow-gepard directory with virtual environment activated.

Note that when using pyenv, you may observe messages telling about `python2` or `python2.7`
not being found when you run the scripts. These are the results of executing gcloud subcommands.
To suppress them, you can set a global shim for `python2.7` (see `pyenv help global`).

### Authentication to Cloud

The scripts use Application Default Credentials (ADC). To find out the possible ways for
authentication, see [Authenticating as a service account](https://cloud.google.com/docs/authentication/production#providing_credentials_to_your_application).

Remember, the account you authenticate with does not have to be the same account as the one used by
the environment you wish to test.

### Environment specification

You need to prepare a json file with the specification of the environment you wish to test.
Specification files will differ depending on the type of environment. Every specification file
must be a dictionary that contains an `environment_type` key with a string indicating the type of
environment. Currently supported environment types:

- COMPOSER - environment running on [Google Cloud Composer](https://cloud.google.com/composer)
- VANILLA_GKE - environment installed directly on a GKE cluster using official helm chart.
  Currently only public clusters are supported.

Specification files may contain jinja variables enclosed in double curly brackets
(for example: `{{variable_name}}`). Values for these variables can be then assigned when running
`airflow_gepard` using `--jinja-variables` argument. `--jinja-variables` accepts a string with
dumped json, where every key and value is also a string. Some variables may also have default values
defined for them, based on the `environment type`.

#### Cloud Composer environment

Every Composer environment specification file can contain following keys:

- `environment_type`: must be equal to "COMPOSER".
- `api_version`: version of Composer Discovery API to use for executing requests.
  Possible choices: "v1", "v1beta1". Defaults to "v1".
- `api_endpoint`: optional string with override of endpoint
  to which composer requests should be sent.
- `force_routing`: optional boolean flag. Setting this flag will cause the connections to the
  private cluster's master endpoint to be made by routing them through one of cluster's nodes, even if
  access to the public endpoint is enabled. If access to the public endpoint is disabled, then routing
  is opened regardless of this flag. Setting this flag may be necessary depending on the machine
  this script is executing on. This flag has no effect for public environments. Defaults to `true`.
- `config_body`: a dictionary with configuration of Composer environment itself. For full list
  of configuration options available under `config_body` key, see
  [Cloud Composer API documentation](http://googleapis.github.io/google-api-python-client/docs/dyn/composer_v1.projects.locations.environments.html#create).

##### Generic specification file

The file [config_generic.json](environments/kubernetes/gke/composer/composer_configurations/config_generic.json)
is a templated specification file that can be used to create many different Composer configurations
by providing values for the variables in the runtime. List of jinja variables defined:

- `api_version`: value for `api_version` explained above, defaults to `"v1"`.
- `api_endpoint`: value for `api_endpoint` explained above, defaults to `""` (an empty string).
- `force_routing`: value for `force_routing` explained above, defaults to `true`.
- `python_version`: the major version of Python used to run the Apache Airflow.
  Can be set to `"2"` or `"3"`, defaults to `"3"`.
- `composer_version`: version of composer add-on to use on the environment (see:
  [Cloud Composer Versioning](https://cloud.google.com/composer/docs/concepts/versioning/composer-versioning-overview)).
  Defaults to `"latest"`.
- `airflow_version`: version of Apache Airflow to use on the environment. Defaults to `"1.10"`.
- `airflow_config_overrides`: a dictionary with overrides for Apache Airflow configuration options.
  Defaults to `{}`,
- `disk_size_gb`: the disk size in GB used for node VMs. Minimum size is 20GB. Defaults to `100`.
- `project_id`: the project id of the environment, nodes and network. Must be provided.
- `zone_id`: the Compute Engine zone in which the nodes will be deployed. Must be provided.
- `machine_type`: the Compute Engine machine type used for cluster instances.
  Defaults to `"n1-standard-2"`.
- `network_id`: the id of the Compute Engine network to be used for machine communications.
  Defaults to `"default"`.
- `location_id`: the Compute Engine location of the environment and subnetwork. Must be provided.
  For the list of locations where Composer is available, see
  [Cloud locations](https://cloud.google.com/about/locations).
- `subnetwork_id`: the id of the Compute Engine subnetwork to be used for machine communications.
  Defaults to `"default"`.
- `use_ip_aliases`: whether or not to enable Alias IPs in the GKE cluster. If `true`, a VPC-native
  cluster is created. Defaults to `false`.
- `node_count`: number of nodes of the GKE cluster. Defaults to `3`.
- `enable_private_environment`: if set to `true`, a Private IP Cloud Composer environment is
  created. If this field is `true`, `use_ip_aliases` must be `true` as well. Defaults to `false`,
- `enable_private_endpoint`: if set to `true`, access to the public endpoint of the GKE cluster is
  denied. Defaults to `false`.
- `master_ipv4_cidr_block`: the IP range in CIDR notation to use for the hosted master network.
  Can only be set to a non empty value for private environments. If left blank, the default value of
  `"172.16.0.0/28"` is used. Defaults to `""`.
- `environment_id`: the name of the Composer environment itself. Must match the pattern:
  `^[a-z](?:[-0-9a-z]{0,62}[0-9a-z])?$` If not provided it will be constructed as
  `"cmp-{composer_version}-af{airflow_version}-py{python_version}-{node_count}nodes"`.
  Note that currently the values of jinja variables will be used to construct this id, not the actual
  values of corresponding configuration options.

The `config_generic.json` file may be copied and filled manually, or the values for the variables
can be provided when running the scripts via `--jinja-variables` argument. Example of usage:
`--jinja-variables '{"force_routing": "true", "airflow_config_overrides": "{\"core-max_active_runs_per_dag\": \"1\"}"}'`

##### Public IP specification

The following files:

- [config_small.json](environments/kubernetes/gke/composer/composer_configurations/config_small.json)
- [config_medium.json](environments/kubernetes/gke/composer/composer_configurations/config_medium.json)
- [config_big.json](environments/kubernetes/gke/composer/composer_configurations/config_big.json)

are representations of a 'small', 'medium' and 'big' public IP environments respectively, each one
twice as big as the previous category (which involves number of nodes, disk size and machine type).
These templated files are simplified versions of `config_generic.json` where some variables were set
and some were removed completely. The only remaining variables are:

- `api_version`
- `api_endpoint`
- `python_version`
- `composer_version`
- `airflow_version`
- `project_id`
- `zone_id`
- `location_id`
- `environment_id`

##### Private IP specification

Private IP environment will be created if
`config_body.config.privateEnvironmentConfig.enablePrivateEnvironment` and
`config_body.config.nodeConfig.ipAllocationPolicy.useIpAliases` keys are set to `true`.
Apart from that, you can also decide if you want to disable access to the public master endpoint
(`config_body.config.privateEnvironmentConfig.privateClusterConfig.enablePrivateEndpoint`
config key set to `true`). Depending on your setup, you might need to use routing
to one of cluster's nodes, which will require additional preparation steps
(see [Routing requirements](#routing-requirements)).

The file [config_small_private.json](environments/kubernetes/gke/composer/composer_configurations/config_small_private.json)
is a private IP version of the `config_small.json` mentioned above. It has four more jinja variables
that can be set:

- `force_routing`
- `network_id`
- `subnetwork_id`
- `enable_private_endpoint`

Also, if you have set up the IAP firewall rule so that it is enabled only for virtual machines with
certain tag, remember to specify the same tag in the list under
`config_body.config.nodeConfig.tags` key.

There are more options for setup of private IP environment that regard mostly
IP address ranges used to allocate IP addresses for pods, services, master network, etc.
Note, that without adjusting them you will not be able to run more than one Composer environment
on the same network and subnetwork at the same time.

#### Vanilla GKE environment

Every Vanilla GKE environment specification file can contain following keys:

- `environment_type`: must be equal to "VANILLA_GKE".
- `project_id`: optional string specifying project id for the cluster. If `None` or an empty string
  are provided then the script will attempt to collect the project id from the ADC. Defaults to `None`.
- `airflow_image_tag`: optional tag from [apache/airflow Docker Hub repository](https://hub.docker.com/r/apache/airflow/tags)
  It specifies a base for a new image containing the copies of elastic DAG file, which will be built
  and pushed to `gcr.io/{project_id}/elastic_dag` repository. Will not be used if `docker_image`
  is provided as well. Defaults to `None`.
- `docker_image`: optional docker registry path to the image to use in a helm chart installation.
  This image should be based on an official apache/airflow image and must already contain copies of
  elastic DAG file in the DAGs folder. The number of copies must be consistent with the value of
  PERF_DAG_FILES_COUNT environment variable from elastic DAG configuration, otherwise the number of
  expected DAGs and Dag Runs will be different than actual number created on environment.
  Either `airflow_image_tag` or `docker_image` must be provided.
  Takes precedence over `airflow_image_tag`. Defaults to `None`.
- `env_variable_sets`: optional dictionary with environment variable name/value pairs to set on
  the cluster. Some Airflow configuration options have predefined default values to set using
  environment variables, but values provided here will take precedence. Defaults to an empty dictionary.
- `helm_chart_sets`: optional dictionary specifying values for helm chart options to set
  when installing Airflow on the cluster. Some helm chart options have default overrides already
  specified in the code, but values provided here will take precedence. Defaults to an empty dictionary.
- `force_routing`: optional boolean flag. Setting this flag will cause the connections to the
  private cluster's master endpoint to be made by routing them through one of cluster's nodes, even if
  access to the public endpoint is enabled. If access to the public endpoint is disabled, then routing
  is opened regardless of this flag. Setting this flag may be necessary depending on the machine
  this script is executing on. This flag has no effect for public environments. Defaults to `true`.
- `cluster_config`: a dictionary with configuration of GKE cluster itself.

##### Generic specification file

The file [config_generic.json](environments/kubernetes/gke/vanilla/configurations/config_generic.json)
is a templated specification file that can be used to create many different Vanilla GKE configurations
by providing values for the variables in the runtime. List of jinja variables defined:

- `project_id`: value for `project_id` explained above. It is also used for specifying which
  network and subnetwork the cluster should use. Must be provided.
- `airflow_image_tag`: value for `airflow_image_tag` explained above, defaults to `1.10.14`.
- `docker_image`: value for `docker_image` explained above, defaults to `""`.
- `env_variable_sets`: value for `env_variable_sets` explained above, defaults to `{}`.
- `helm_chart_sets`: value for `helm_chart_sets` explained above, defaults to `{}`.
- `force_routing`: value for `force_routing` explained above, defaults to `true`.
- `cluster_id`: name of the GKE cluster where Airflow will be installed. Must match the pattern:
  `^[a-z](?:[-0-9a-z]{0,38}[0-9a-z])?$` If not provided it will be constructed as
  `"af{airflow_version}-{node_count}nodes"`. Note that currently the values of jinja variables will
  be used to construct this id, not the actual values of corresponding configuration options.
- `zone_id`: the Compute Engine zone in which the nodes of the cluster will be deployed. Must be provided.
- `machine_type`: the Compute Engine machine type used for cluster instances.
  Defaults to `"n1-standard-2"`.
- `disk_size_gb`: the disk size in GB used for node VMs. Minimum size is 20GB. Defaults to `100`.
- `node_count`: number of nodes of the GKE cluster. Defaults to `3`.
- `max_pods_per_node`: the default max number of pods per node for node pools in the cluster.
  Can be specified only for private clusters and will be dropped for non-private ones. Defaults to `32`.
- `network_id`: the id of the Compute Engine network to be used for machine communications.
  Defaults to `"default"`.
- `location_id`: the Compute Engine location of the subnetwork. Must be provided.
- `subnetwork_id`: the id of the Compute Engine subnetwork to be used for machine communications.
  Defaults to `"default"`.
- `use_ip_aliases`: whether or not to enable Alias IPs in the GKE cluster. If `true`, a VPC-native
  cluster is created. Defaults to `false`.
- `enable_private_nodes`: if set to `true`, a Private IP cluster is created. If this field is `true`
  `use_ip_aliases` must be `true` as well. Defaults to `false`.
- `enable_private_endpoint`: if set to `true`, access to the public endpoint of the GKE cluster is
  denied. Defaults to `false`.
- `master_ipv4_cidr_block`: the IP range in CIDR notation to use for the hosted master network.
  Can only be set to a non empty value for private environments. Non-empty value must be provided
  for private clusters. Defaults to `""`.

Similarly to its Composer equivalent, the `config_generic.json` file may be copied and filled
manually, or the values for the variables can be provided when running the scripts via
`--jinja-variables` argument.

##### Public IP specification

The following file:

- [config_small.json](environments/kubernetes/gke/composer/composer_configurations/config_small.json)

is a representations of a 'small' public IP environment. This templated file is a simplified version
of `config_generic.json` where some variables were set and some were removed completely.
The only remaining variables are:

- `airflow_image_tag`
- `project_id`
- `docker_image`
- `env_variable_sets`
- `helm_chart_sets`
- `cluster_id`
- `zone_id`
- `location_id`

##### Private IP specification

**Vanilla GKE environments using private clusters are currently not supported.**

Private IP environment will be created if
`cluster_config.private_cluster_config.enable_private_nodes`
and `cluster_config.ip_allocation_policy.use_ip_aliases` keys are set to `true`.
Apart from that, you can also decide if you want to disable access to the public master endpoint
(`cluster_config.private_cluster_config.enable_private_endpoint` config key set to `true`).
Depending on your setup, you might need to use routing to one of cluster's nodes, which will require
additional preparation steps (see [Routing requirements](#routing-requirements)).

The file [config_small_private.json](environments/kubernetes/gke/vanilla/configurations/config_small_private.json)
is a private IP version of the `config_small.json` mentioned above. It has five more jinja variables
that can be set:

- `force_routing`
- `network_id`
- `location_id`
- `subnetwork_id`
- `enable_private_endpoint`

Also, if you have set up the IAP firewall rule so that it is enabled only for virtual machines with
certain tag, remember to specify the same tag in the list under
`cluster_config.node_pools.config.tags` key.

Once again, there are more options for setup of private IP environment that regard mostly
IP address ranges used to allocate IP addresses for pods, services, master network, etc.
Note, that without adjusting them you will not be able to run more than one Vanilla GKE
on the same network and subnetwork at the same time.

### Elastic DAG

[Elastic DAG](performance_dags/elastic_dag/elastic_dag.py) is currently the only supported way of
uploading DAGs to the test environment. It is a special DAG file that allows to create
different test scenarios via providing following environment variables:

- PERF_DAG_FILES_COUNT: number of copies of elastic DAG file that should be uploaded to the test
  environment. Every copy will use the same configuration for DAGs (except for dag_id prefix).
  Must be a positive int (default: "1").
- PERF_DAG_PREFIX: string that will be used as a prefix for filename of every DAG file copy
  and as a prefix of dag_id for every DAG created (default: "perf_scheduler").
- PERF_DAGS_COUNT: number of DAGs that should be created from every copy of DAG file.
  Must be a positive int (required).
- PERF_TASKS_COUNT: number of tasks for every DAG. Must be a positive int (required).
- PERF_START_AGO: a string identifying a duration (eg. 2h13m) that will be subtracted
  from current time to determine start_date of DAGs (default: "1h").
- PERF_START_DATE: a date string in "%Y-%m-%d %H:%M:%S.%f" format identifying the start_date of the
  DAGs. If not provided, it will be calculated based on the value of PERF_START_AGO and added to the
  configuration.
- PERF_SCHEDULE_INTERVAL: schedule_interval for every DAG. Can be either "@once"
  or an expression following the same format as for PERF_START_AGO (default: "@once")
- PERF_SHAPE: a string which determines structure of every DAG.
  Possible values: "no_structure", "linear", "binary_tree", "star", "grid" (required).
- PERF_SLEEP_TIME: a float non-negative value that specifies sleep time in seconds for every task
  (default: "0").
- PERF_OPERATOR_TYPE: a string specifying type of operator to use: "bash" for BashOperator,
  "python" for PythonOperator or "big_query_insert_job" for BigQueryInsertJobOperator (default: "bash")
- PERF_MAX_RUNS: number of DagRuns that should be created for every DAG. This will be achieved
  by setting DAGs' end_date according to start_date and schedule_interval.
  The resulting end_date cannot occur in the future.
  Must be specified if PERF_SCHEDULE_INTERVAL is provided as a time expression
  and cannot be specified if it is not.
  Must be a positive int (default: not specified).
- PERF_START_PAUSED: value determining whether DAGs should be initially paused.
  Must be an integer: 0 will be interpreted as `False`, and every other value as `True` (default: "1").
- PERF_TASKS_TRIGGER_RULE: value defining the rule by which dependencies are applied for the tasks
  to get triggered. Options are defined in the class airflow.utils.trigger_rule.TriggerRule.
- PERF_OPERATOR_EXTRA_KWARGS: json value defining extra kwargs for DAG tasks

Elastic dag configuration has to be set up in such a way, that a number of scheduled Dag Runs is
finite and all of them can be scheduled at the time of test execution (no runs will have start
date occurring in the future). Values for all variables must be provided as strings.
If not required variables are not specified, then the default value will be used.
Before the performance test is started, validation of environment variables is run using
`validate_elastic_dag_conf` method from [elastic_dag_utils.py](performance_dags/elastic_dag/elastic_dag_utils.py)
module which ensures that elastic dag configuration fulfills requirements.

Environment variables can be provided in the main configuration file
(under "config" -> "softwareConfig" -> "envVariables" section)
or in a dict in a separate json file via --elastic-dag-config-file-path argument
(in which case they will override any values specified in the main file as well).
Examples of configurations for elastic DAG can be found
in [elastic_dag_configurations](performance_dags/elastic_dag/elastic_dag_configurations) directory.

### Running the performance tests

#### Single test attempt

To run a single test attempt, use the command explained below:

<!-- AIRFLOW_GEPARD_AUTO_START -->

```
  usage: airflow_gepard [-h] -s environment_specification_file_path
                        [-j JINJA_VARIABLES] [-e ELASTIC_DAG_CONFIG_FILE_PATH]
                        [-c RESULTS_COLUMNS] [-o OUTPUT_PATH] [-r RESULTS_OBJECT_NAME]
                        [--reuse-if-exists] [--delete-if-exists]
                        [--delete-upon-finish] [--reset-environment]

  optional arguments:
    -h, --help            show this help message and exit
    -s environment_specification_file_path, --environment-specification-file-path environment_specification_file_path
                          Path to the json file with specification of the
                          environment you wish to create and test. Your file can
                          contain jinja variables, in which case you must
                          additionally provide their values using jinja-
                          variables argument.
    -j JINJA_VARIABLES, --jinja-variables JINJA_VARIABLES
                          String containing dumped json with jinja variables for
                          templated specification file. For example: if your
                          specification file contains {{project_id}} variable,
                          you must provide its value when running the script by
                          adding following argument to the command: --jinja-
                          variables '{"project_id": "my-project"}'
    -e ELASTIC_DAG_CONFIG_FILE_PATH, --elastic-dag-config-file-path ELASTIC_DAG_CONFIG_FILE_PATH
                          Path to the json file with configuration of
                          environment variables used by the elastic dag. You can
                          provide the environment variables for the elastic dag
                          using this argument or directly in the main
                          specification file (the one specified with
                          --environment-specification-file-path). Note that
                          values from --elastic-dag-config-file path will
                          override any variables with the same names from the
                          main specification file.
    -c RESULTS_COLUMNS, --results-columns RESULTS_COLUMNS
                          String containing dumped json with column name/value
                          pairs that should be added to the results table. Value
                          for any column from this argument will overwrite the
                          column with the same name already present in the
                          results table. New columns will be added at the
                          beginning of the table. Example: --results-columns
                          '{"user": "Me"}'
    -o OUTPUT_PATH, --output-path OUTPUT_PATH
                          Local path to a directory or a file where results
                          dataframe should be saved in csv format. If path to a
                          directory is provided, then default filename will be
                          constructed based on environment configuration.
    -r RESULTS_OBJECT_NAME, --results-object-name RESULTS_OBJECT_NAME
                          Name to use for results file. If not provided,
                          then file name will generated based on the contents
                          of results table.
    --reuse-if-exists     Setting this flag will cause the script to reuse an
                          existing environment with the same name as the one
                          specified in your specification json file rather than
                          recreate it. By default, the script will exit if such
                          an environment already exists (unless it is already in
                          deleting state - in such case it will patiently wait
                          for this environment's deletion). If --reset-
                          environment flag was also set, then Airflow's metadata
                          database of the environment will be burned down and
                          rebuilt before reusing it. Note that configuration of
                          existing environment (including elastic DAG
                          configuration) might differ from the one specified in
                          configuration files. This flag takes precedence over
                          --delete-if-exists.
    --delete-if-exists    Setting this flag will cause the script to delete an
                          existing environment with the same name as the one
                          specified in your specification json file and then
                          recreate it. By default, the script will exit if such
                          an environment already exists (unless it is already in
                          deleting state - in such case it will patiently wait
                          for this environment's deletion).
    --delete-upon-finish  Setting this flag will cause the script to delete the
                          environment once it finishes execution. It will also
                          cause the environment to be deleted if it's creation
                          finished with an error which may allow for
                          investigation of an issue.
    --reset-environment   Setting this flag will result in the reset of the
                          existing environment (currently only the refresh of
                          Airflow metadata database). Without it, the
                          environment will be used as is (without any cleaning
                          operations). This may result in an unexpected
                          behavior. This flag has effect only if --reuse-if-
                          exists flag was set as well.
```

<!-- AIRFLOW_GEPARD_AUTO_END -->

For now, the script allows only to test the Composer environment and Vanilla GKE environment. It will:

- prepare the environment instance (possibly reusing or deleting an existing one)
- upload copies of the elastic dag to it once the environment is ready
- wait for all expected test DAGs to be parsed by the scheduler
- unpause the test DAGs
- wait for all the expected test Dag Runs to finish (with either a success or failure)
- write the performance metrics in a csv format to the results table
- upload the results table to a local csv file, Google Cloud Storage bucket and/or BigQuery dataset
- delete the environment (if `--delete-upon-finish` flag was set).

Notes:

- if your specification file contains jinja variables that do not have default values, you must
  specify them via `--jinja-variables`

#### Multiple test attempts

To run multiple performance tests in parallel, you can use the script
[run_multiple_performance_tests.py](run_multiple_performance_tests.py).
Its arguments are described below:

<!-- MULTIPLE_RUNS_AUTO_START -->

```
  usage: run_multiple_performance_tests.py [-h] -s STUDY_FILE_PATH
                                           [-m MAX_CONCURRENCY]
                                           [--reports-bucket REPORTS_BUCKET]
                                           [--reports-project-id REPORTS_PROJECT_ID]
                                           [-u SCRIPT_USER] [-c CI_BUILD_ID]
                                           [-d LOGS_DIRECTORY]
                                           [-p LOGS_PROJECT_ID] [-b LOGS_BUCKET]

  optional arguments:
    -h, --help            show this help message and exit
    -s STUDY_FILE_PATH, --study-file-path STUDY_FILE_PATH
                          Path to the json file containing the definition of the
                          study, which describes the test attempts that should
                          be run (their specification, arguments and number of
                          attempts).
    -e ELASTIC_DAG_CONFIG_FILE_PATH, --elastic-dag-config-file-path ELASTIC_DAG_CONFIG_FILE_PATH
                          Path to the json file with configuration of
                          environment variables used by the elastic dag. You can
                          provide the environment variables for the elastic dag
                          using this argument or directly in the main
                          specification file (the one specified in study args).
                          Note that values from --elastic-dag-config-file path
                          will override any variables with the same names from
                          the main specification file.
    -m MAX_CONCURRENCY, --max-concurrency MAX_CONCURRENCY
                          Maximal number of test attempts this script may run at
                          the same time. Must be a positive integer.
    --reports-bucket REPORTS_BUCKET
                          Name of the GCS bucket where the directory with
                          reports will be uploaded This argument can also be
                          provided in the study file.
    --reports-project-id REPORTS_PROJECT_ID
                          Google cloud project id. It is used to specify where
                          the GCS bucket for reports should be created in case
                          it does not exist (reports-bucket argument). If not
                          provided, the script will attempt to collect the
                          project id from the ADC. This argument can also be
                          provided in the study file.
    -u SCRIPT_USER, --script-user SCRIPT_USER
                          Name of the user who executed this script. If
                          provided, it will override 'user' column in the
                          results tables (it will override 'user' entry of
                          'results_columns' arg for every study component).
    -c CI_BUILD_ID, --ci-build-id CI_BUILD_ID
                          [DEPRECATED] Id of the automatic CI build that
                          triggered this script. If provided, it will override
                          'ci_build_id' column in the results tables (it will
                          override 'ci_build_id' entry of 'results_columns' arg
                          for every study component).
    -d LOGS_DIRECTORY, --logs-directory LOGS_DIRECTORY
                          [DEPRECATED] Path to the directory in which the logs
                          of test attempts will be stored - one log for every
                          test attempt. This argument can also be provided in
                          the study file.
    -p LOGS_PROJECT_ID, --logs-project-id LOGS_PROJECT_ID
                          [DEPRECATED] Google cloud project id. It is used to
                          specify where the GCS bucket for logs should be
                          created in case it does not exist (logs-bucket
                          argument). If not provided, the script will attempt to
                          collect the project id from the ADC. This argument can
                          also be provided in the study file.
    -b LOGS_BUCKET, --logs-bucket LOGS_BUCKET
                          [DEPRECATED] Name of the GCS bucket where the logs of
                          test attempts will be stored: one blob for every
                          executed test attempt. This argument can also be
                          provided in the study file.
```

<!-- MULTIPLE_RUNS_AUTO_END -->

##### Study file

This script requires providing a file with a definition of a study via `--study-file-path` argument.
Study is a meant to be a combination of environments performance of which can be compared to draw
some conclusions. For example [composer_sizes.json](studies/various_sizes.json) contains
the definition of a study meant to examine the influence of size of GKE cluster on the performance,
while [composer_public_and_private.json](studies/public_and_private.json) is supposed to
check the differences between public and private IP environment that are otherwise the same
(note that these two studies require providing `project_id` in `jinja_variables` argument before
they can be run).

You can specify `airflow_gepard` arguments for every component of a study individually. You can also
define the number of attempts every study component should be repeated to allow for averaging
of results. The script also allows you to control the number of parallel tests running with
`--max-concurrency` argument (which defaults to 5). Since most of the work during performance test
is executed in external systems, parallelism is achieved without multiprocessing. The script will
simply initialize `PerformanceTest` class for every test attempt and will periodically check their
status until they reach the terminal state.

Every study file is a dict that can contain following keys at a top level:

- `study_components` - list containing dicts with definitions of subsequent study components. Each
  study component is a dictionary, that can contain following keys: - `component_name` - name given to this study component. Will be used in report creation. If not
  provided, a name will be given to it based on the type of environment used by component and its
  order in the `study_components` list. `component_name` mus be unique for every study component. - `baseline_component_name` - optional `component_name` of another component, which is supposed
  to serve as a baseline to given component's performance. Specifying a baseline will cause
  an additional comparison report to be generated. `baseline_component_name` and `component_name`
  of given study component cannot be the same. - `args` - dictionary with values of `airflow_gepard` script arguments. Only arguments which
  accept a value should be specified in `args`. The 'dumped json' type arguments
  (`--jinja-variables` and `--results-columns`), must be specified as dictionaries instead of
  strings. Argument names must be specified using their destinations in the python code (for
  example, argument `--results-dataset` becomes `results_dataset` in the `args` dict). Note that
  `output_path` and `results_bucket` are not supported and cannot be specified. `results_dataset`
  is the only supported way to store the results and must be provided. Any value provided for
  `results_object_name` will be overridden by the one created by the script. - `flags` - list of flag type arguments (boolean arguments that are set without providing
  value for them, for example `--reuse-if-exists`) that should be set for this component.
  Similarly to `args`, flags must be provided using their destinations in the python code. - `attempts` - a total number of environments that should be created for given study component. - `randomize_environment_name` - a boolean argument. If set to `false`, it will cause the random
  part to not be added to the environment name. Because of that it is currently required for such
  component to have a single attempt, otherwise an exception will be thrown. Defaults to `true`
- `default_args` - dictionary with default values of `airflow_gepard` script arguments for every
  study component. If one of the study components does not define an argument explicitly, then its
  value from `default_args` is used (if defined). In case of 'dumped json' type arguments, their value
  from `default_args` will be combined with their explicit value, instead of being overridden
  completely.
- `default_flags` - list of flag type arguments that should be set for every study component,
  unless one specifies its own list.
- `default_attempts` - default number of attempts every study component should be run, unless one
  specifies its own number of attempts. If not provided, defaults to 1.
- `reports_bucket` - the GCS bucket where the directories with contents of performance tests results
  analysis will be stored. Is overridden by `--reports-bucket` argument of the script, but otherwise
  has same purpose. The `reports_bucket` must be provided either in the study file or via argument.
- `reports_project_id` - project id for the `reports_bucket`. Will be used only if the provided
  bucket does not exist. If not provided, the script will use the project id from the ADC.
  Is overridden by `--reports-project-id` argument of the script, but otherwise has same purpose.
- `logs_directory` - `[DEPRECATED]` the local path to store the logs, separate file for every test
  attempt. Is overridden by `--logs-directory` argument of the script, but otherwise has same purpose.
- `logs_bucket` - `[DEPRECATED]` the GCS bucket to store the logs, separate blob for every test
  attempt. The script will create the bucket if it does not exist. Is overridden by `--logs-bucket`
  argument of the script, but otherwise has same purpose.
- `logs_project_id` - `[DEPRECATED]` project id for the `logs_bucket`.
  Is overridden by `--logs-project-id` argument of the script, but otherwise has same purpose.

Let's take a look at the contents of an exemplary study file
[composer_study_example.json](studies/composer_study_example.json) to determine, what arguments
will be used when starting test attempts.

1.  first study component: 1. `component_name`: this component is called `"component_1"` 1. `args`: two arguments specified in dict explicitly: - `environment_specification_file_path` - relative path leading to
    [config_big.json](environments/kubernetes/gke/composer/composer_configurations/config_big.json). - `jinja_variables` - provided as dictionary with only one key: `environment_id`. Since the
    `jinja_variables` is a 'dumped json' type argument and it is also defined in `default_args`, then
    the final value is a dict being combination of these two:

                ```python
              {
                  "environment_id": "big-env",
                  "project_id": "my-project-id",
                  "location_id": "europe-west1",
                  "zone_id": "europe-west1-b"
              }
                ```
            Value for remaining argument `results_dataset` will be taken from
            `default_args`.
        1. `flags`: because this component does not specify its own `flags`, `default_flags` list
        (equal to `["delete_if_exists", "delete_upon_finish"]`) is used.
        1. `attempts`: because this component does not specify its own `attempts`, `default_attempts`
        (equal to `4`) are used instead.
        1. `randomize_environment_name`: because this component does not specify this field, a default
        value of `true` will be used. It means that a random string will be added to `environment_id`
        (`"big-env"`) when constructing environment name for every test attempt of this component.

1.  second study component: 1. `component_name`: this component is called `"component_2"` 1. `baseline_component_name`: `"component_1"` is the baseline to which performance of this
    component will be compared 1. `args`: three arguments specified in dict explicitly: - `environment_specification_file_path` - relative path leading to
    [config_small.json](environments/kubernetes/gke/composer/composer_configurations/config_small.json). - `results_dataset` - a BQ dataset called `"another_results_dataset"`,
    which overrides the value from `default_args` - `jinja_variables` - a dictionary with two keys: `environment_id` and `project_id`.
    `project_id` provided here overrides the value from `jinja_variables` dict in `default_args`:

                ```python
              {
                  "environment_id": "small-env",
                  "project_id": "another-project-id",
                  "location_id": "europe-west1",
                  "zone_id": "europe-west1-b"
              }
                ```
        1. `flags`: this component specifies its own `flags` list (equal to `["delete_if_exists"]`),
        which is used instead of `default_flags`.
        1. `attempts`: this component specifies its own `attempts` (equal to `1`),
        which are used instead of `default_attempts`.
        1. `randomize_environment_name`: this component has explicitly specified not to add a random
        part to `environment_id` (`"small-env"`) when constructing environment name. Setting this flag
        will not cause the script to fail, as number of attempts for this study component was set to
        `1`, so environment names will not overlap.

    After all test attempts finish their execution, reports will be generated and stored in
    `bucket_for_perf_test_reports` GCS bucket. A total of three reports will be generated: one
    individual report for every study component and one comparison report.
    Additionally, "statistics" table is populated with aggregated results from all test attempts.

Additional notes:

- script runs thorough analysis of arguments and specification files validation before running any
  attempt, but some issues can only be discovered upon starting the test attempt execution. It is
  recommended to test the specifications using `airflow-gepard` script first.
- relative file path `environment_specification_file_path` is considered to be relative to the study file location.
- the only supported way of storing the results is BQ dataset (`results_dataset`). Providing
  `output_path` or `results_bucket` for any of the components will cause the study validation to fail.
- results of all test attempts belonging to the same study component are saved in the same BQ table
  (`results_object_name` argument). The table name is generated by the script and follows the format:
  `{study_id}__{environment_type}__{component_name}`, where: - `study_id` - an id of the study execution. It is a timestamp in format `%Y%m%d_%H_%M_%S`,
  generated at the start of the script. - `environment_type` - type of the environment used by the component. - `component_name` is the name of the study component that can be provided in study file. If
  name for given component was not provided, then name in format `{environment_type}_{index}` will
  be used, where `index` corresponds to the order of components with the same `environment_type`
  in study file.
- for now, all test attempts for private IP Composer environments are run in sequence. It is
  necessary to run them with `delete_upon_finish` flag to allow next attempt to reuse the IP ranges.
  It is also not recommended to test private IP Composer environments using this script if you have to
  use routing to connect to them, as routing subprocesses will be started every time
  private environment's status is checked, which can slow down the script.
- by default, the script will add a random part to the name of every environment it creates
  to make sure the names do not overlap. You can disable this behavior for a study component by
  setting `randomize_environment_name` to `false` in its configuration (see example above). You still
  need to make sure number of attempts for this component is set to `1` and no other component will
  use the same environment name. This is especially useful if you want an existing environment
  to be used by the script (by additionally providing `reuse_if_exists` flag).

## Code reference

### Performance test as a state machine

A single performance test attempt is represented by [PerformanceTest](performance_test.py) class.
This class is responsible for:

- initializing the correct class representing the tested environment (based on the value under
  `environment_type` from `environment-specification-file-path` file)
- starting and progressing the test with `check_state` method, which checks the current state
  of the tested environment and executes environment's method corresponding to this state. The
  environment's method also returns the next state of the environment which is set by `check_state`.
  `check_state` is also responsible for handling terminal states and retries. All retryable states
  will be retried up to two times, after which the environment moves to the terminal `FAILED` state.
- saving the results with `save_results` method once the environment reaches one of its
  successful terminal states.

Class for each type of environment must derive from [BaseEnvironment](environments/base_environment.py)
abstract class. `BaseEnvironment` defines the interface which must be implemented by the environment
class in order to be able to run performance test with it. Its most important method is `states_map`
which is supposed to return a dictionary that for every applicable environment state defines an
[Action](environments/base_environment.py) to be performed in order to progress the test. `Action` is a tuple containing the method
to be called, the proposed sleep time after executing said method and a boolean flag telling
whether it is possible to retry the method in case of an error.

Currently, all types of environments use the same set of states, represented by enum class
[State](environments/base_environment.py), but not every state has to be used by given environment.
Some states, however, must be used:

- `NONE` - an initial state for every environment of any type
- `DONE` - a terminal state indicating a successful execution of performance test
- `FAILED` - a terminal state indicating an unsuccessful execution of performance test
- `FAILED_DO_NOT_DELETE` - same as `FAILED`, except this state also indicates, that the environment
  should not be deleted

For example, in case of [ComposerEnvironment](environments/kubernetes/gke/composer/composer_environment.py)
the graph state is as follows:

```
                                    -----------
                                    |         |     DEPLOY_STATS_COLLECTOR
  ----------------------- WAIT_UNTIL_READY <---       ^          |
  |                               ^      |            |          |
  |                               |      ---> UPDATE_ENV_INFO    ->UPLOAD_DAG --> WAIT_FOR_DAG --> UNPAUSE_DAG --> WAIT_FOR_DAG_RUN_EXEC --> COLLECT_RESULTS --> DONE
  |                               |                  |                 ^           |       ^                            |       ^
  |                               |                  |                 |           |       |                            |       |
  |                               |                  ----> RESET_ENV ---           ---------                            ---------
  |                               |
  |   FAILED_DO_NOT_DELETE <--- NONE <-------------------------------------------------
  |                               |                                                   |
  |                               |                                                   |
  |                               ---> WAIT_UNTIL_CAN_BE_DELETED ---> DELETING_ENV <---
  |                                        |        |       ^           |       ^
  |                                        |        |       |           |       |
  ---> FAILED <-----------------------------        ---------           ---------

```

1. `NONE` - the starting state with `prepare_environment` as state method. Its results
   depends on the flags set and whether the Composer instance with desired name already exists. - if Composer instance does not exist, it sends a request creating it and moves to
   `WAIT_UNTIL_READY` state - if Composer instance exists but is being deleted, it just moves to `DELETING_ENV` state - if Composer instance exists, is not being deleted and `--reuse-if-exists` flag was set,
   it moves to `WAIT_UNTIL_READY` state - if Composer instance exists, is not being deleted, `--delete-if-exists` flag was set
   and `--reuse-if-exists` flag was not set, it moves to `WAIT_UNTIL_CAN_BE_DELETED` state - if neither of the above applies, it moves to `FAILED_DO_NOT_DELETE`, which is terminal state
1. `WAIT_UNTIL_READY` - state indicating the wait for Composer instance to be ready to use. Executes
   `is_composer_instance_ready` method, which:
   - returns the same state as long as the Composer instance is at `UPDATING` or `CREATING` state
   - moves to `UPDATE_ENV_INFO` state if Composer instance reaches `RUNNING` state
   - moves to `FAILED` state if Composer instance reaches any other state, at which point the test
     ends
1. `DELETING_ENV` - state indicating the wait Composer instance to be deleted. It's state method,
   `_wait_for_deletion`, returns the same state until Composer ceases to exist, at which point it
   moves to `NONE` state.
1. `WAIT_UNTIL_CAN_BE_DELETED` - state similar to `WAIT_UNTIL_READY` (it has the same state method
   assigned) but it waits for Composer instance to be deletable instead of usable:
   - returns the same state as long as the Composer instance is at `UPDATING` or `CREATING` state
   - sends a request deleting Composer instance if it is in `RUNNING` or `ERROR` state, then moves
     to `DELETING_ENV` state
   - moves to `FAILED` state if Composer instance reaches any other state, at which point the test
     ends
1. `UPDATE_ENV_INFO` - state with `_update_environment_info` method assigned, which updates
   `ComposerEnvironment` object attributes with details that become available only once Composer
   instance is created (like the name of GKE cluster). In the end if both `--reuse-if-exists` and
   `--reset-environment` flags were set, it moves to `RESET_ENV` state, otherwise to `UPLOAD_DAG` state
1. `RESET_ENV` - has `_reset_environment` method assigned, which runs a remote operation on
   `airflow-worker` pod which resets airflow database, then moves `UPLOAD_DAG` state
1. `UPLOAD_DAG` - its state method `upload_dag_files` upload copies of elastic DAG file to the
   dag storage of Composer instance, then moves to `WAIT_FOR_DAG` state
1. `WAIT_FOR_DAG` - it uses the `check_if_dags_have_loaded` method of `ComposerEnvironment` base
   class, [GKEBasedEnvironment](environments/kubernetes/gke/gke_based_environment.py). It runs a remote
   operation on `airflow-worker` pod that checks if all expected DAGs have been parsed by the scheduler.
   If they did, the environment will move to `UNPAUSE_DAG` state, otherwise it remains in `WAIT_FOR_DAG`
   state.
1. `UNPAUSE_DAG` - runs `unpause_dags` method `GKEBasedEnvironment` class. It runs a remote
   operation on `airflow-worker` pod that changes the `is_paused` attribute of all test DAGs to `False`.
1. `WAIT_FOR_DAG_RUN_EXEC` - uses `check_dag_run_execution_status` method of `GKEBasedEnvironment`
   class. It checks how many Dag Runs of test DAGs have reached `success` or `failed` state. If that
   number is lower than a total number of expected Dag Runs, environment remains in this state,
   otherwise it moves to `COLLECT_RESULTS` state.
1. `COLLECT_RESULTS` - runs `collect_results` method of `GKEBasedEnvironment` class. It collects
   configuration data of the environment, Airflow configuration and Dag Run statistics as well as Cloud
   Monitoring time series metrics and combines them all into results table. Then it moves to
   `DONE` state.
1. `DONE` - a terminal state without any method assigned. It indicates that the test has finished
   successfully and results can be saved.

### Communicating with pods on GKE cluster

During performance test execution, there is a lot of communication with the pods on the GKE cluster.
The communication is done via [RemoteRunner](environments/kubernetes/remote_runner.py) class. This
class used `run_command` to run bash commands and `run_python_trampoline` to execute python code
remotely on specified pods. This is similar in effect as using `kubectl exec` command. The methods
that execute remote commands on pods are:

- `reset_airflow_database` - resets Airflow database
- `get_dags_count` - checks the number of parsed dags
- `unpause_dags` - unpauses test dags
- `get_dag_runs_count` - checks the number of finished dag runs
- `get_airflow_configuration` - collects the Airflow configuration
- `get_python_version` - collects exact python version used
- `collect_dag_run_statistics` - collects test Dag Run time statistics
- `get_airflow_version` - collects Airflow version installed on instance (Vanilla GKE only)

Most of these methods prepare corresponding python code snippets (so called `trampolines`)
that are sent to pods and executed remotely. These snippets are stored in
[airflow_pod_operations](environments/kubernetes/airflow_pod_operations.py)
module. During runtime, the code of methods provided there are put together with their arguments
into a `PYTHON_TRAMPOLINE_TEMPLATE`, and the resulting string is sent to a pod. This is why it is
crucial that each of these snippets contains all imports required by it directly inside the method.
It also allows to avoid the need for installing the dependencies of these methods in gepard - thanks
to that gepard does not require installing airflow itself.

In case of `ComposerEnvironment` and `VanillaGKEEnvironment`, the `RemoteRunner` instances are
provided by [GKERemoteRunnerProvider](environments/kubernetes/gke/gke_remote_runner_provider.py).
This class uses `get_remote_runner` method to create and return `RemoteRunner` object inside of
an isolated context. This ensures that connection with the GKE cluster is established using the
provided credentials without causing any side effects (like changing the kubernetes context for
current user).

### Preparing results table

Once the test Dag Runs are finished, the results table can be created. For example, in case of
`ComposerEnvironment`, the results table contents are collected in `COLLECT_RESULTS` state
by following methods:

- `prepare_environment_columns` of `ComposerEnvironment` - collects columns with environment
  configuration, like composer and airflow version
- `get_airflow_configuration` of `RemoteRunner` - collects columns with a chosen set of Airflow
  configuration options
- `prepare_elastic_dag_columns` of `GKEBasedEnvironment` - returns the columns with configuration
  of elastic DAG environment variables
- `collect_airflow_statistics` of `GKEBasedEnvironment` - collects statistics of test Dag Runs
  execution, like total test duration, average Dag Run execution time or a total number of task
  instances executed

Each of these methods returns an `OrderedDict` so that order of columns in the results table is not
random. The dictionaries are combined with the time series metrics into the results table by
`prepare_results_dataframe` method from [results_dataframe](environments/kubernetes/gke/collecting_results/results_dataframe.py)
module.

#### Cloud Monitoring time series metrics

The base for the results table is the metrics dataframe, returned by `prepare_metrics_dataframe` method
from [metrics_dataframe](environments/kubernetes/gke/collecting_results/metrics_dataframe.py) module.
The metrics collected from Cloud Monitoring are defined in `get_monitored_resources_map` method from
[monitored_resources](environments/kubernetes/gke/collecting_results/monitoring_api/monitored_resources.py)
module. Currently, the collected metrics belong to three resource types:

- k8s_node
- k8s_pod
- k8s_container

For each of these resource types the map contains a list of resource groups and a list of metrics.

1. Resource group is a dictionary that controls which instances of given resource
   (which nodes, pods or containers) will have metrics collected. It can contain following keys: - `resource_label_filters` - an optional dictionary that contains filters on resource labels
   (for example: cluster name), that will be applied when collecting time series. Not specifying
   any filters will cause collecting time series for all resources of given type (all nodes, pods
   or containers, from any cluster within given project). - `aggregation` - a boolean flag, telling if collected time series should be aggregated into a
   single one. If set to `True`, collected time series are first aligned to full minutes so that
   their points have matching timestamps, then time series are aggregated by summing them.
   `GAUGE` metrics are aligned by calculating mean of point values within the time frame.
   `CUMULATIVE` and `DELTA` metrics are aligned by calculating the rate at which their values
   change. This causes the resulted aggregated time series to change the metric kind to `GAUGE`. To
   reflect this change, `_per_second` suffix is also added to the metric name. Defaults to `False`. - `new_resource_labels` - an optional dictionary where new values for resource labels can be
   specified, which will be added to collected time series. It is primarily designed to be used for
   aggregated time series, which lose their original label information upon aggregation.
1. Metric is a dictionary describing the metric of collected time series. It can contain following
   keys: - `metric_type` - an obligatory name of the metric - `metric_label_filters` - an optional dictionary that contains filters on metric labels
   (for example: memory type) that will be applied when collecting time series. Not every metric
   type has applicable labels. It is important to collect metrics with different metric label values
   separately, so that in case of aggregated resource groups only time series with the same label
   value are summed.

When collecting the time series in [MonitoringApi](environments/kubernetes/gke/collecting_results/monitoring_api/monitoring_api.py)
a call is made for every possible pair of resource group and metric. Once all time series are
collected, they are joined into a single dataframe that resembles a fact table. In this table each
row contains metric values for a single timestamp and a single resource. The same timestamp can
appear in multiple rows, but it should then refer to different resources (different nodes, pods
or containers).

### Results table

The resulting table will contain following columns:

1.  General:
    - `uuid` - unique id of test attempt generated based on environment name and test start date
    - `user` - name of current user that is running the performance test scripts
1.  Environment configuration:

    A set of columns describing the configuration of tested environment. Different for each type
    of environment.

    - Composer environment
      - `environment_name` - name of the environment
      - `environment_type` - equal to COMPOSER
      - `composer_version` - version of composer add-on used on environment
      - `airflow_version` - version of Apache Airflow used on environment
      - `python_version` - full version of python used on environment
      - `gke_version` - version of GKE used on the cluster
      - `composer_api_version` - version of composer discovery api used for requests
      - `composer_api_endpoint` - composer api endpoint override used for requests
      - `environment_size` - size category of the environment. If the size does not match
        any of defined categories, then "custom" size category is returned.
      - `node_count` - number of nodes in the GKE cluster
      - `machine_type` - Compute Engine machine type used for GKE cluster instance
      - `disk_size_gb` - disk size in GB used for node VMs
      - `private_ip_enabled` - TRUE if given instance uses Private IP and FALSE otherwise
      - `drs_enabled` - TRUE if given instance has DRS enabled and FALSE otherwise
    - Vanilla GKE environment
      - `environment_name` - id of the GKE cluster
      - `environment_type` - equal to VANILLA_GKE
      - `airflow_version` - version of Apache Airflow used on environment
      - `python_version` - full version of python used on environment
      - `gke_version` - version of GKE used on the cluster
      - `environment_size` - size category of the environment. If the size does not match
        any of defined categories, then "custom" size category is returned.
      - `node_count` - number of nodes in the GKE cluster
      - `machine_type` - Compute Engine machine type used for GKE cluster instance
      - `disk_size_gb` - disk size in GB used for node VMs
      - `private_ip_enabled` - TRUE if given instance uses Private IP and FALSE otherwise

1.  Airflow configuration:

    - `AIRFLOW__CORE__DAG_CONCURRENCY`
    - `AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT`
    - `AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG`
    - `AIRFLOW__CORE__PARALLELISM`
    - `AIRFLOW__CORE__STORE_SERIALIZED_DAGS`
    - `AIRFLOW__CELERY__WORKER_AUTOSCALE`
    - `AIRFLOW__CELERY__WORKER_CONCURRENCY`
    - `AIRFLOW__SCHEDULER__MAX_THREADS`

    The names of these columns follow
    [the format of Airflow environment variables](https://airflow.readthedocs.io/en/stable/howto/set-config.html)
    and contain values of matching Airflow configuration options. For more insight on the meaning of
    each of the configuration options, see [Airflow Configuration Reference](https://airflow.apache.org/docs/stable/configurations-ref.html).

1.  Elastic DAG configuration:

    - `elastic_dag_configuration_type` - column composed from the values of elastic DAG variables
      that follows the form `{PERF_SHAPE}__{PERF_DAG_FILES_COUNT}_dag_files__{PERF_DAGS_COUNT}_dags__{PERF_TASKS_COUNT}_tasks__{PERF_MAX_RUNS}_dag_runs__{PERF_SLEEP_TIME}_sleep__{PERF_OPERATOR_TYPE}_operator`
    - `PERF_DAG_FILES_COUNT`
    - `PERF_DAGS_COUNT`
    - `PERF_TASKS_COUNT`
    - `PERF_MAX_RUNS`
    - `PERF_SCHEDULE_INTERVAL`
    - `PERF_SHAPE`
    - `PERF_SLEEP_TIME`
    - `PERF_OPERATOR_TYPE`

    These columns contain the values of corresponding elastic DAG environment variables (explained
    above). Note, that not all of the variables are included in the results.

1.  Airflow statistics:

    - `test_start_date` - earliest `start_date` of any Dag Runs created as part of the test
    - `test_end_date` - latest `end_date` of any Dag Runs created as part of the test
    - `test_duration` - the difference between `test_end_date` and `test_start_date` in seconds
    - `dag_run_total_count` - total amount of test Dag Runs
    - `dag_run_success_count` - amount of test Dag Runs that finished with a success
    - `dag_run_failed_count` - amount of test Dag Runs that finished with a failure
    - `dag_run_average_duration` - average duration of test Dag Runs, where duration is calculated
      as difference between Dag Run's `end_date` and `start_date`.
    - `dag_run_min_duration` - minimal duration of any of test Dag Runs
    - `dag_run_max_duration` - maximal duration of any of test Dag Runs
    - `task_instance_total_count` - total amount of Task Instances belonging to test Dag Runs
    - `task_instance_average_duration` - average `duration` of test Task Instances
    - `task_instance_min_duration` - minimal `duration` of any of test Task Instances
    - `task_instance_max_duration` - minimal `duration` of any of test Task Instances

    Note that only Dag Runs which have both `end_date` and `start_date` other than `None`
    take part in calculating Dag Run's duration, while only Task Instance
    which have `duration` other than `None` take part in calculating Task Instance's duration.

1.  Cloud Monitoring time series:

    - resource label columns

      - `project_id`
      - `location`
      - `cluster_name`
      - `node_name`
      - `namespace_name`
      - `pod_name`
      - `container_name`

      These columns identify the resource to which values presented in metric columns in given row
      (in given `timestamp`) relate.

      Apart from raw time series data, the results table also contains aggregated time series
      which were obtained by aligning (in 60 seconds windows) and summing time series of
      resources belonging to specific groups. These aggregated time series can be recognized by:

      - `nodes_combined` value in `node_name` column - time series combining all nodes of the
        cluster. Contains values only for `k8s_node` metrics.
      - `airflow_workers_combined` value in `pod_name` column - time series combining all
        `airflow-worker` pods AND containers. Contains values for `k8s_pod` and `k8s_container`
        metrics.
      - `airflow_schedulers_combined` value in `pod_name` column - time series combining all
        `airflow-scheduler` pods AND containers. Contains values for `k8s_pod` and
        `k8s_container` metrics.

    - `timestamp` - time of measurement provided as seconds passed since the Unix Epoch
    - `seconds_from_normalized_start` - amount of seconds that passed since `test_start_date`
      (rounded down to full minutes) until `timestamp`
    - metric columns - multiple columns with values of metrics collected from Cloud Monitoring.
      Names of these columns consist from following parts, separated by double underscores: - `{resource_type}` - the type of monitored resource, for example: `k8s_node` - `{metric_kind}` - kind of metric: `DELTA`, `CUMULATIVE` or `GAUGE` - `{metric_type}` - the type of metric, for example: `kubernetes.io/node/cpu/total_cores`.

          Since aggregated time series for `DELTA` and `CUMULATIVE` metrics are aligned by calculating
          the rate at which they change, the suffix `_per_second` is added to their `{metric_type}`.
          - `{metric_label}-{metric_label_value}` - name and value of the label corresponding to
          metric joined by a hyphen, for example: `memory_type-evictable`,
          where `memory_type` is `{metric_label}` and `evictable` is `{metric_label_value}`.
          Amount of labels (if any) varies depending on the `{metric_type}` and every pair
          is separated by double underscores in resulting metric column name.

Additionally the results table can contain any extra columns provided with `--results-columns`
argument. These additional column will be added in sorted order at the beginning of the table.

### Automatic report contents

Report generation is currently only execute when running performance test via `run_multiple_performance_tests.py`.
Reports are generated once all test attempts of the study finish their execution. An individual
report is generated for each study component using the template [report.md.tpl](../reports/report.md.tpl).
Specifying a `baseline_component_name` for any of the study components also causes a comparison
report to be generated, that uses the template [comparison_report.md.tpl](../reports/comparison_report.md.tpl).

Each report contains the following:

- detailed information about configuration of test attempts belonging to study component(s),
  including the count of anomalous test attempts and test attempts with failed dag runs, which
  are removed from results analysis
- a description of metrics presented in the report
- a table with average values of aggregated metrics calculated across all test attempts belonging
  to the study component(s) (and their comparison in case of comparison reports)
- time series metric charts, showing how time series metrics changed in time, separately line for
  every test attempt belonging to the study component(s)
- boxplot charts, showing the overall range of values of the time series metrics
  across all test attempts belonging to the study component(s).

The directory with reports is saved under `reports_bucket` GCS bucket provided when running
the study. For example, below tou can see the structure of the reports directory for the exemplary
study file [composer_study_example.json](studies/composer_study_example.json):

```
.
+-- {study_id}
    +-- individual_reports
    |    +-- component_1
    |    |    +-- component_1.md
    |    |    +-- component_1.pdf
    |    |    +-- boxplot_charts
    |    |    |    +-- k8s_node__GAUGE__kubernetes_io_node_cpu_core_usage_time_per_second_box_plot.png
    |    |    |    +-- k8s_node__GAUGE__kubernetes_io_node_memory_used_bytes__memory_type_evictable_box_plot.png
    |    |    |    +-- k8s_node__GAUGE__kubernetes_io_node_memory_used_bytes__memory_type_non_evictable_box_plot.png
    |    |    |    +-- k8s_node__GAUGE__kubernetes_io_node_network_received_bytes_count_per_second_box_plot.png
    |    |    |    +-- k8s_node__GAUGE__kubernetes_io_node_network_sent_bytes_count_per_second_box_plot.png
    |    |    +-- time_series_charts
    |    |         +-- all__k8s_node__GAUGE__kubernetes_io_node_cpu_core_usage_time_per_second_time_series_chart.png
    |    |         +-- all__k8s_node__GAUGE__kubernetes_io_node_memory_used_bytes__memory_type_evictable_time_series_chart.png
    |    |         +-- all__k8s_node__GAUGE__kubernetes_io_node_memory_used_bytes__memory_type_non_evictable_time_series_chart.png
    |    |         +-- all__k8s_node__GAUGE__kubernetes_io_node_network_received_bytes_count_per_second_time_series_chart.png
    |    |         +-- all__k8s_node__GAUGE__kubernetes_io_node_network_sent_bytes_count_per_second_time_series_chart.png
    |    +-- component_2
    |        ...
    +-- comparison_reports
        +-- component_1__AND__component_2
            +-- component_1__AND__component_2.md
            +-- component_1__AND__component_2.pdf
            +-- boxplot_charts
            |    +-- k8s_node__GAUGE__kubernetes_io_node_cpu_core_usage_time_per_second_box_plot.png
            |    +-- k8s_node__GAUGE__kubernetes_io_node_memory_used_bytes__memory_type_evictable_box_plot.png
            |    +-- k8s_node__GAUGE__kubernetes_io_node_memory_used_bytes__memory_type_non_evictable_box_plot.png
            |    +-- k8s_node__GAUGE__kubernetes_io_node_network_received_bytes_count_per_second_box_plot.png
            |    +-- k8s_node__GAUGE__kubernetes_io_node_network_sent_bytes_count_per_second_box_plot.png
            +-- time_series_charts
                 +-- all__k8s_node__GAUGE__kubernetes_io_node_cpu_core_usage_time_per_second_time_series_chart.png
                 +-- all__k8s_node__GAUGE__kubernetes_io_node_memory_used_bytes__memory_type_evictable_time_series_chart.png
                 +-- all__k8s_node__GAUGE__kubernetes_io_node_memory_used_bytes__memory_type_non_evictable_time_series_chart.png
                 +-- all__k8s_node__GAUGE__kubernetes_io_node_network_received_bytes_count_per_second_time_series_chart.png
                 +-- all__k8s_node__GAUGE__kubernetes_io_node_network_sent_bytes_count_per_second_time_series_chart.png
```

`comparison_reports` directory will appear only if at least one `baseline_component_name` was
specified. `study_id` is the same as the one used in the `results_object_name` (described above).

In order to see the full contents of the `.md` report, you must download both the `.md` file and
the directories with `.png` images. The `.pdf` reports will be present only if you have installed
`wkhtmltopdf` binary.

## Running locally on dev Composer

In order to execute performance tests on dev Composer running executor locally you can use run_dev.sh script.

```
# Prepare your virtual environment and install dependencies from root directory.
pip install -e .

# Make sure you have set authorized gcloud account for composer-performance-tests project via
# gcloud config set account or GOOGLE_APPLICATION_CREDENTIALS environment variable.
# export GOOGLE_APPLICATION_CREDENTIALS=...

# Execute tests.
./run_dev.sh
```
