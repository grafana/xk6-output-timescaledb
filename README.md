# xk6-output-timescaledb

Is an output for k6 which sends metrics to timescaledb in a predefined schema.

# Install
You will need [go](https://golang.org/)

```
go install go.k6.io/xk6/cmd/xk6@latest
xk6 build --with github.com/grafana/xk6-output-timescaledb
```

You will have a `k6` binary in the currect directory.

# Docker

There is an included Dockerfile that build a docker image.

# Docker-compose

The included docker-compose.yml starts timescaledb, grafana and k6. This is just a quick to setup to show the usage, for real use case you might want to deploy outside of docker, use volumes and probably update versions.

Put your script in the `scripts` directory, run `docker-compose up`, wait for it to start both timescaledb and grafana and then run `docker-compose run k6 /script/yourscript.js --tag testid=<someid>`. Open `http://localhost:3000/d/a21-pyAWz/open-source-load-testing-stack` in a browser and see your results.

# Configuration

You need to configure a [result output](https://k6.io/docs/using-k6/options/#results-output) with name `timescaledb`, the configuration after the `=` is postgres [connection string](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING)

Examples with an env variable:
```
K6_OUT=timescaledb=postgresql://k6:k6@timescaledb:5432/k6 k6 run script.js
```
or with an cli flag
```
k6 run -o timescaledb=postgresql://k6:k6@timescaledb:5432/k6 script.js
```

There is an additional configuration option `K6_TIMESCALEDB_PUSH_INTERVAL` which can configure how often metrics are pushed with a default value of `1s` or 1 second.
