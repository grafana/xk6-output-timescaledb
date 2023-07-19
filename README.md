# xk6-output-timescaledb

`xk6-output-timescaledb` is a [k6 extension](https://k6.io/docs/extensions/) to send k6 metrics to TimescaleDB in a predefined schema.

# Install

You will need [go](https://golang.org/)

```bash

# Install xk6
go install go.k6.io/xk6/cmd/xk6@latest

# Build the k6 binary
xk6 build --with github.com/grafana/xk6-output-timescaledb

... [INFO] Build environment ready
... [INFO] Building k6
... [INFO] Build complete: ./k6
```
You will have a `k6` binary in the current directory.

**Using Docker**

This [Dockerfile](./Dockerfile) builds a docker image with the k6 binary.


# Configuration


First, find the [Postgres connection string](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING) of the TimescaleDB instance.

To run the test and send the k6 metrics to TimescaleDB, use the `k6 run` command setting the [k6 output option](https://k6.io/docs/using-k6/options/#results-output) as `timescaledb=YOUR_POSTGRES_CONNECTION_STRING`. For example:


```bash
k6 run -o timescaledb=postgresql://k6:k6@timescaledb:5432/k6 script.js
```

or set an environment variable:

```bash
K6_OUT=timescaledb=postgresql://k6:k6@timescaledb:5432/k6 k6 run script.js
```

## Options

The `xk6-output-timescaledb` extension supports this additional option:

- `K6_TIMESCALEDB_PUSH_INTERVAL`: to define how often metrics are sent to TimescaleDB.  The default value is `1s` (1 second).

# Docker Compose

This repo includes a [docker-compose.yml](./docker-compose.yml) file that starts TimescaleDB, Grafana, and a custom build of k6 having the `xk6-output-timescaledb` extension. This is just a quick to setup to show the usage, for real use case you might want to deploy outside of docker, use volumes and probably update versions.

Clone the repo to get started and follow these steps: 

1. Put your k6 scripts in the `samples` directory or use the `http_2.js` example.

3. Start the docker compose environment.
	```shell
	docker compose up -d
	```

	```shell
	# Output
	Creating xk6-output-timescaledb_grafana_1     ... done
	Creating xk6-output-timescaledb_k6_1          ... done
	Creating xk6-output-timescaledb_timescaledb_1 ... done
	```

4. Use the k6 Docker image to run the k6 script and send metrics to the TimescaleDB container started on the previous step. You must [set the `testid` tag](https://k6.io/docs/using-k6/tags-and-groups/#test-wide-tags) with a unique identifier to segment the metrics into discrete test runs for the [Grafana dashboards](#dashboards).
    ```shell
    docker compose run --rm -T k6 run -<samples/http_2.js --tag testid=<SOME-ID>
    ```
   For convenience, the `docker-run.sh` can be used to simply:
    ```shell
    ./docker-run.sh samples/http_2.js
    ```

	> Note that the [docker-compose command to run k6 tests](https://k6.io/docs/getting-started/running-k6/) might differ depending your OS.

5. Visit http://localhost:3000/ to view results in Grafana.

## Dashboards

The docker-compose setup comes with two pre-built dashboards. One for listing the discrete test runs as a list, and the other for visualizing the results of a specific test run.

### Test list dashboard

![Dashboard of test runs](./images/dashboard-test-runs.png)

### Test result dashboard

![Dashboard of test result](./images/dashboard-test-result.png)
