package timescaledb

import (
	"testing"
	"time"

	"go.k6.io/k6/lib/types"
	"gopkg.in/guregu/null.v3"

	"github.com/stretchr/testify/assert"
)

func Test_getConsolidatedConfig_succeeds(t *testing.T) {
	actualConfig, err := getConsolidatedConfig(
		[]byte(`{"pg_url":"postgres://user:password@localhost:5433/mydbname","push_interval":"2s"}`),
		map[string]string{
			"K6_TIMESCALEDB_URL":           "postgres://user:password@localhost:5433/mydbname",
			"K6_TIMESCALEDB_PUSH_INTERVAL": "2s",
		},
		"postgres://user:password@localhost:5433/mydbname?push_interval=2s")
	assert.NoError(t, err)
	assert.Equal(t, config{
		PgUrl:        null.StringFrom("postgres://user:password@localhost:5433/mydbname"),
		PushInterval: types.NullDurationFrom(2 * time.Second),
		addr:         null.StringFrom("postgres://localhost:5433"),
		dbName:       null.StringFrom("mydbname"),
	}, actualConfig)
}

func Test_getConsolidatedConfig_from_json_and_populates_config_fields_from_json_url(t *testing.T) {
	actualConfig, err := getConsolidatedConfig(
		[]byte(`{"pg_url":"postgres://user:password@localhost:5433/mydbname","push_interval":"2s"}`),
		nil,
		"")
	assert.NoError(t, err)
	assert.Equal(t, config{
		PgUrl:        null.StringFrom("postgres://user:password@localhost:5433/mydbname"),
		PushInterval: types.NullDurationFrom(2 * time.Second),
		addr:         null.StringFrom("postgres://localhost:5433"),
		dbName:       null.StringFrom("mydbname"),
	}, actualConfig)
}

func Test_getConsolidatedConfig_from_env_variables(t *testing.T) {
	actualConfig, err := getConsolidatedConfig(
		nil,
		map[string]string{
			"K6_TIMESCALEDB_URL":           "postgres://user:password@localhost:5433/mydbname",
			"K6_TIMESCALEDB_PUSH_INTERVAL": "2s",
		},
		"")

	assert.NoError(t, err)
	assert.Equal(t, config{
		PgUrl:        null.StringFrom("postgres://user:password@localhost:5433/mydbname"),
		PushInterval: types.NullDurationFrom(2 * time.Second),
		addr:         null.StringFrom("postgres://localhost:5433"),
		dbName:       null.StringFrom("mydbname"),
	}, actualConfig)
}

func Test_getConsolidatedConfig_env_variable_takes_precedence_without_config_arg(t *testing.T) {
	actualConfig, err := getConsolidatedConfig(
		[]byte(`{"pg_url":"postgres://user:password@localhost:1111/jsonDBName","push_interval":"5s","db_name":"jsonDBName"}`),
		map[string]string{
			"K6_TIMESCALEDB_URL":           "postgres://user:password@localhost:5433/mydbname",
			"K6_TIMESCALEDB_PUSH_INTERVAL": "2s",
		},
		"")

	assert.NoError(t, err)
	assert.Equal(t, config{
		PgUrl:        null.StringFrom("postgres://user:password@localhost:5433/mydbname"),
		PushInterval: types.NullDurationFrom(2 * time.Second),
		addr:         null.StringFrom("postgres://localhost:5433"),
		dbName:       null.StringFrom("mydbname"),
	}, actualConfig)
}

func Test_getConsolidatedConfig_config_argument_takes_precedence(t *testing.T) {
	actualConfig, err := getConsolidatedConfig(
		[]byte(`{"pg_url":"postgres://jsonUser:jsonPassword@localhost:1111/jsonDBName","push_interval":"5s","db_name":"jsonDBName"}`),
		map[string]string{
			"K6_TIMESCALEDB_URL":           "postgres://user:password@localhost:5433/mydbname",
			"K6_TIMESCALEDB_PUSH_INTERVAL": "2s",
		},
		"postgres://confUser:confPassword@localhost:2222/confDBName?push_interval=8s")

	assert.NoError(t, err)
	assert.Equal(t, config{
		PgUrl:        null.StringFrom("postgres://confUser:confPassword@localhost:2222/confDBName"),
		PushInterval: types.NullDurationFrom(time.Duration(8000000000)),
		addr:         null.StringFrom("postgres://localhost:2222"),
		dbName:       null.StringFrom("confDBName"),
	}, actualConfig)
}

func Test_getConsolidatedConfig_returns_default_values(t *testing.T) {
	actualConfig, err := getConsolidatedConfig(nil, nil, "")

	assert.NoError(t, err)
	assert.Equal(t, config{
		PgUrl:        null.NewString("postgresql://localhost/myk6timescaleDB", false),
		PushInterval: types.NewNullDuration(time.Duration(1000000000), false),
		addr:         null.NewString("postgresql://localhost", false),
		dbName:       null.NewString("myk6timescaleDB", false),
	}, actualConfig)
}

func Test_getConsolidatedConfig_returns_error_for_invalid_json(t *testing.T) {
	_, err := getConsolidatedConfig([]byte(`invalid`), nil, "")
	assert.Error(t, err)
}

func Test_getConsolidatedConfig_returns_error_for_invalid_json_url(t *testing.T) {
	_, err := getConsolidatedConfig([]byte(`{"pg_url":"http://foo.com/?foo\nbar"}`), nil, "")
	assert.Error(t, err)
}

func Test_getConsolidatedConfig_returns_error_for_invalid_env_url(t *testing.T) {
	_, err := getConsolidatedConfig(nil, map[string]string{
		"K6_TIMESCALEDB_URL": "http://foo.com/?foo\nbar",
	}, "")
	assert.Error(t, err)
}

func Test_getConsolidatedConfig_returns_error_for_invalid_env_push_interval(t *testing.T) {
	_, err := getConsolidatedConfig(nil, map[string]string{
		"K6_TIMESCALEDB_PUSH_INTERVAL": "invalid",
	}, "")
	assert.Error(t, err)
}

func Test_getConsolidatedConfig_returns_error_for_invalid_config_argument_url(t *testing.T) {
	_, err := getConsolidatedConfig(nil, nil, "http://foo.com/?foo\nbar")

	assert.Error(t, err)
}

func Test_parselUrl_succeeds(t *testing.T) {
	actualConfig, err := parseUrl("postgres://user:password@localhost:5433/mydbname?push_interval=2s")

	assert.NoError(t, err)
	assert.Equal(t, config{
		PgUrl:        null.StringFrom("postgres://user:password@localhost:5433/mydbname"),
		PushInterval: types.NullDurationFrom(2 * time.Second),
		addr:         null.StringFrom("postgres://localhost:5433"),
		dbName:       null.StringFrom("mydbname"),
	}, actualConfig)
}

func Test_parselUrl_returns_error_for_unknown_query(t *testing.T) {
	_, err := parseUrl("postgres://user:password@localhost:5433/mydbname?push_interval=2s&unknown=value")

	assert.Error(t, err)
}

func Test_parselUrl_returns_error_for_invalid_push_interval(t *testing.T) {
	_, err := parseUrl("postgres://user:password@localhost:5433/mydbname?push_interval=invalid")

	assert.Error(t, err)
}

func Test_parselUrl_returns_error_for_invalid_input(t *testing.T) {
	_, err := parseUrl("http://foo.com/?foo\nbar")

	assert.Error(t, err)
}
