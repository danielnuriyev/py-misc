## Install

```shell script
pip install --upgrade "dask[complete]"
pip install --upgrade "prefect[all_extras]"
```

## Start

### Start Dask:

```shell script
dask-scheduler
dask-worker <address of dask scheduler from above>
```

### Start Prefect

Set up server as described [here](https://docs.prefect.io/core/getting_started/installation.html#running-the-local-server-and-ui)

Start the server and an agent:

```shell script
prefect server start
prefect agent start
```

