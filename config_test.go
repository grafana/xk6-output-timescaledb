package timescaledb

import (
	"testing"
	"time"

	"go.k6.io/k6/lib/types"
	"gopkg.in/guregu/null.v3"

	"github.com/stretchr/testify/assert"
)

func Test_getConsolidatedConfig_Succeeds(t *testing.T) {
	actualConfig, err := getConsolidatedConfig(
		[]byte(`{"url":"postgres://user:password@localhost:5433/mydbname","pushInterval":"2s"}`),
		map[string]string{
			"K6_TIMESCALEDB_URL":           "postgres://user:password@localhost:5433/mydbname",
			"K6_TIMESCALEDB_PUSH_INTERVAL": "2s",
		},
		"postgres://user:password@localhost:5433/mydbname?pushInterval=2s")
	assert.NoError(t, err)
	assert.Equal(t, config{
		URL:          null.StringFrom("postgres://user:password@localhost:5433/mydbname"),
		PushInterval: types.NullDurationFrom(2 * time.Second),
		addr:         null.StringFrom("postgres://localhost:5433"),
		dbName:       null.StringFrom("mydbname"),
	}, actualConfig)
}

func Test_getConsolidatedConfig_FromJsonAndPopulatesConfigFieldsFromJsonUrl(t *testing.T) {
	actualConfig, err := getConsolidatedConfig(
		[]byte(`{"url":"postgres://user:password@localhost:5433/mydbname","pushInterval":"2s"}`),
		nil,
		"")
	assert.NoError(t, err)
	assert.Equal(t, config{
		URL:          null.StringFrom("postgres://user:password@localhost:5433/mydbname"),
		PushInterval: types.NullDurationFrom(2 * time.Second),
		addr:         null.StringFrom("postgres://localhost:5433"),
		dbName:       null.StringFrom("mydbname"),
	}, actualConfig)
}

func Test_getConsolidatedConfig_FromEnvVariables(t *testing.T) {
	actualConfig, err := getConsolidatedConfig(
		nil,
		map[string]string{
			"K6_TIMESCALEDB_URL":           "postgres://user:password@localhost:5433/mydbname",
			"K6_TIMESCALEDB_PUSH_INTERVAL": "2s",
		},
		"")

	assert.NoError(t, err)
	assert.Equal(t, config{
		URL:          null.StringFrom("postgres://user:password@localhost:5433/mydbname"),
		PushInterval: types.NullDurationFrom(2 * time.Second),
		addr:         null.StringFrom("postgres://localhost:5433"),
		dbName:       null.StringFrom("mydbname"),
	}, actualConfig)
}

func Test_getConsolidatedConfig_EnvVariableTakesPrecedenceWithoutConfigArg(t *testing.T) {
	actualConfig, err := getConsolidatedConfig(
		[]byte(`{"url":"postgres://user:password@localhost:1111/jsonDBName","pushInterval":"5s","db_name":"jsonDBName"}`),
		map[string]string{
			"K6_TIMESCALEDB_URL":           "postgres://user:password@localhost:5433/mydbname",
			"K6_TIMESCALEDB_PUSH_INTERVAL": "2s",
		},
		"")

	assert.NoError(t, err)
	assert.Equal(t, config{
		URL:          null.StringFrom("postgres://user:password@localhost:5433/mydbname"),
		PushInterval: types.NullDurationFrom(2 * time.Second),
		addr:         null.StringFrom("postgres://localhost:5433"),
		dbName:       null.StringFrom("mydbname"),
	}, actualConfig)
}

func Test_getConsolidatedConfig_ConfigArgumentTakesPrecedence(t *testing.T) {
	actualConfig, err := getConsolidatedConfig(
		[]byte(`{"url":"postgres://jsonUser:jsonPassword@localhost:1111/jsonDBName","pushInterval":"5s","db_name":"jsonDBName"}`),
		map[string]string{
			"K6_TIMESCALEDB_URL":           "postgres://user:password@localhost:5433/mydbname",
			"K6_TIMESCALEDB_PUSH_INTERVAL": "2s",
		},
		"postgres://confUser:confPassword@localhost:2222/confDBName?pushInterval=8s")

	assert.NoError(t, err)
	assert.Equal(t, config{
		URL:          null.StringFrom("postgres://confUser:confPassword@localhost:2222/confDBName"),
		PushInterval: types.NullDurationFrom(time.Duration(8000000000)),
		addr:         null.StringFrom("postgres://localhost:2222"),
		dbName:       null.StringFrom("confDBName"),
	}, actualConfig)
}

func Test_getConsolidatedConfig_ReturnsDefaultValues(t *testing.T) {
	actualConfig, err := getConsolidatedConfig(nil, nil, "")

	assert.NoError(t, err)
	assert.Equal(t, config{
		URL:          null.NewString("postgresql://localhost/myk6timescaleDB", false),
		PushInterval: types.NewNullDuration(time.Duration(1000000000), false),
		addr:         null.NewString("postgresql://localhost", false),
		dbName:       null.NewString("myk6timescaleDB", false),
	}, actualConfig)
}

func Test_getConsolidatedConfig_ReturnsErrorForInvalidJson(t *testing.T) {
	_, err := getConsolidatedConfig([]byte(`invalid`), nil, "")
	assert.Error(t, err)
}

func Test_getConsolidatedConfig_ReturnsErrorForInvalidJsonUrl(t *testing.T) {
	_, err := getConsolidatedConfig([]byte(`{"url":"http://foo.com/?foo\nbar"}`), nil, "")
	assert.Error(t, err)
}

func Test_getConsolidatedConfig_ReturnsErrorForInvalidEnvUrl(t *testing.T) {
	_, err := getConsolidatedConfig(nil, map[string]string{
		"K6_TIMESCALEDB_URL": "http://foo.com/?foo\nbar",
	}, "")
	assert.Error(t, err)
}

func Test_getConsolidatedConfig_ReturnsErrorForInvalidEnvPushInterval(t *testing.T) {
	_, err := getConsolidatedConfig(nil, map[string]string{
		"K6_TIMESCALEDB_PUSH_INTERVAL": "invalid",
	}, "")
	assert.Error(t, err)
}

func Test_getConsolidatedConfig_ReturnsErrorForInvalidConfigArgumentUrl(t *testing.T) {
	_, err := getConsolidatedConfig(nil, nil, "http://foo.com/?foo\nbar")

	assert.Error(t, err)
}

func Test_parselUrl_Succeeds(t *testing.T) {
	actualConfig, err := parseURL("postgres://user:password@localhost:5433/mydbname?pushInterval=2s")

	assert.NoError(t, err)
	assert.Equal(t, config{
		URL:          null.StringFrom("postgres://user:password@localhost:5433/mydbname"),
		PushInterval: types.NullDurationFrom(2 * time.Second),
		addr:         null.StringFrom("postgres://localhost:5433"),
		dbName:       null.StringFrom("mydbname"),
	}, actualConfig)
}

func Test_parselUrl_ReturnsErrorForUnknownQuery(t *testing.T) {
	_, err := parseURL("postgres://user:password@localhost:5433/mydbname?pushInterval=2s&unknown=value")

	assert.Error(t, err)
}

func Test_parselUrl_ReturnsErrorForInvalidPushInterval(t *testing.T) {
	_, err := parseURL("postgres://user:password@localhost:5433/mydbname?pushInterval=invalid")

	assert.Error(t, err)
}

func Test_parselUrl_ReturnsErrorForInvalidInput(t *testing.T) {
	_, err := parseURL("http://foo.com/?foo\nbar")

	assert.Error(t, err)
}
