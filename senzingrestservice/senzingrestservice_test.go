package senzingrestservice

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	api "github.com/docktermj/go-rest-api-service/senzingrestapi"
	"github.com/senzing/go-common/g2engineconfigurationjson"
	"github.com/stretchr/testify/assert"
)

var (
	restApiServiceSingleton RestService
)

// ----------------------------------------------------------------------------
// Internal functions
// ----------------------------------------------------------------------------

func getTestObject(ctx context.Context, test *testing.T) RestService {
	if restApiServiceSingleton == nil {
		senzingEngineConfigurationJson, err := g2engineconfigurationjson.BuildSimpleSystemConfigurationJson("")
		if err != nil {
			test.Errorf("Error: %s", err)
		}
		restApiServiceSingleton = &RestApiServiceImpl{
			SenzingEngineConfigurationJson: senzingEngineConfigurationJson,
			SenzingModuleName:              "go-rest-api-service-test",
			SenzingVerboseLogging:          0,
		}
	}
	return restApiServiceSingleton
}

func testError(test *testing.T, ctx context.Context, err error) {
	if err != nil {
		test.Log("Error:", err.Error())
		assert.FailNow(test, err.Error())
	}
}

// ----------------------------------------------------------------------------
// Test harness
// ----------------------------------------------------------------------------

func TestMain(m *testing.M) {
	err := setup()
	if err != nil {
		fmt.Print(err)
		os.Exit(1)
	}
	code := m.Run()
	err = teardown()
	if err != nil {
		fmt.Print(err)
	}
	os.Exit(code)
}

func setup() error {
	var err error = nil
	return err
}

func teardown() error {
	var err error = nil
	return err
}

// ----------------------------------------------------------------------------
// Test interface functions
// ----------------------------------------------------------------------------

func TestRestApiServiceImpl_AddDataSources(test *testing.T) {
	ctx := context.TODO()
	dataSourceName := fmt.Sprintf("DS-%d", time.Now().Unix())
	testObject := getTestObject(ctx, test)
	request := &api.AddDataSourcesReqApplicationJSON{}
	params := api.AddDataSourcesParams{
		DataSource: []string{dataSourceName},
	}
	response, err := testObject.AddDataSources(ctx, request, params)
	testError(test, ctx, err)
	switch responseTyped := response.(type) {
	case *api.SzDataSourcesResponse:
		if false {
			drillDown := []interface{}{
				response,
				responseTyped,
				responseTyped.Data,
				responseTyped.Data.Value,
				responseTyped.Data.Value.DataSourceDetails,
				responseTyped.Data.Value.DataSourceDetails.Value,
				responseTyped.Data.Value.DataSourceDetails.Value["xxxBob"],
				responseTyped.Data.Value.DataSourceDetails.Value["xxxBob"].DataSourceCode,
				responseTyped.Data.Value.DataSourceDetails.Value["xxxBob"].DataSourceCode.Value,
			}

			for index, value := range drillDown {
				test.Logf(">>>>> %d: %-60s %+v\n", index, reflect.TypeOf(value), value)
			}
		}
	}
}

func TestRestApiServiceImpl_Heartbeat(test *testing.T) {
	ctx := context.TODO()
	testObject := getTestObject(ctx, test)
	response, err := testObject.Heartbeat(ctx)
	testError(test, ctx, err)
	httpMethod, err := response.Meta.Value.HttpMethod.Value.MarshalText()
	testError(test, ctx, err)
	assert.Equal(test, "GET", string(httpMethod))
}

// ----------------------------------------------------------------------------
// Examples for godoc documentation
// ----------------------------------------------------------------------------

func ExampleRestApiServiceImpl_Heartbeat() {

}
