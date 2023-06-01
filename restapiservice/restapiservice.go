package restapiservice

import (
	"context"
	"fmt"
	"reflect"

	// "fmt"
	"sync"
	"time"

	api "github.com/docktermj/go-rest-api-client/senzingrestapi"
	"github.com/senzing/g2-sdk-go/g2api"
	"github.com/senzing/go-logging/logging"
	"github.com/senzing/go-observing/observer"
	"github.com/senzing/go-sdk-abstract-factory/factory"
	"google.golang.org/grpc"
)

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

// RestApiServiceImpl is...
type RestApiServiceImpl struct {
	api.UnimplementedHandler
	abstractFactory                factory.SdkAbstractFactory
	abstractFactorySyncOnce        sync.Once
	g2configmgrSingleton           g2api.G2configmgr
	g2configmgrSyncOnce            sync.Once
	g2configSingleton              g2api.G2config
	g2configSyncOnce               sync.Once
	GrpcDialOptions                []grpc.DialOption
	GrpcTarget                     string
	isTrace                        bool
	logger                         logging.LoggingInterface
	LogLevelName                   string
	ObserverOrigin                 string
	Observers                      []observer.Observer
	Port                           int
	SenzingEngineConfigurationJson string
	SenzingModuleName              string
	SenzingVerboseLogging          int
}

// ----------------------------------------------------------------------------
// Variables
// ----------------------------------------------------------------------------

var debugOptions []interface{} = []interface{}{
	&logging.OptionCallerSkip{Value: 5},
}

var traceOptions []interface{} = []interface{}{
	&logging.OptionCallerSkip{Value: 5},
}

var defaultModuleName string = "init-database"

// ----------------------------------------------------------------------------
// internal methods
// ----------------------------------------------------------------------------

// --- Logging ----------------------------------------------------------------

// Get the Logger singleton.
func (restApiService *RestApiServiceImpl) getLogger() logging.LoggingInterface {
	var err error = nil
	if restApiService.logger == nil {
		loggerOptions := []interface{}{
			&logging.OptionCallerSkip{Value: 3},
		}
		restApiService.logger, err = logging.NewSenzingToolsLogger(ComponentId, IdMessages, loggerOptions...)
		if err != nil {
			panic(err)
		}
	}
	return restApiService.logger
}

// Log message.
func (restApiService *RestApiServiceImpl) log(messageNumber int, details ...interface{}) {
	restApiService.getLogger().Log(messageNumber, details...)
}

// Debug.
func (restApiService *RestApiServiceImpl) debug(messageNumber int, details ...interface{}) {
	details = append(details, debugOptions...)
	restApiService.getLogger().Log(messageNumber, details...)
}

// Trace method entry.
func (restApiService *RestApiServiceImpl) traceEntry(messageNumber int, details ...interface{}) {
	restApiService.getLogger().Log(messageNumber, details...)
}

// Trace method exit.
func (restApiService *RestApiServiceImpl) traceExit(messageNumber int, details ...interface{}) {
	restApiService.getLogger().Log(messageNumber, details...)
}

// --- Errors -----------------------------------------------------------------

// Create error.
func (restApiService *RestApiServiceImpl) error(messageNumber int, details ...interface{}) error {
	return restApiService.getLogger().NewError(messageNumber, details...)
}

// --- Services ---------------------------------------------------------------

func (restApiService *RestApiServiceImpl) getAbstractFactory() factory.SdkAbstractFactory {
	restApiService.abstractFactorySyncOnce.Do(func() {
		if len(restApiService.GrpcTarget) == 0 {
			restApiService.abstractFactory = &factory.SdkAbstractFactoryImpl{}
		} else {
			restApiService.abstractFactory = &factory.SdkAbstractFactoryImpl{
				GrpcDialOptions: restApiService.GrpcDialOptions,
				GrpcTarget:      restApiService.GrpcTarget,
				ObserverOrigin:  restApiService.ObserverOrigin,
				Observers:       restApiService.Observers,
			}
		}
	})
	return restApiService.abstractFactory
}

// Singleton pattern for g2config.
// See https://medium.com/golang-issue/how-singleton-pattern-works-with-golang-2fdd61cd5a7f
func (restApiService *RestApiServiceImpl) getG2config(ctx context.Context) g2api.G2config {
	var err error = nil
	restApiService.g2configSyncOnce.Do(func() {
		restApiService.g2configSingleton, err = restApiService.getAbstractFactory().GetG2config(ctx)
		if err != nil {
			panic(err)
		}
		if restApiService.g2configSingleton.GetSdkId(ctx) == factory.ImplementedByBase {
			err = restApiService.g2configSingleton.Init(ctx, restApiService.SenzingModuleName, restApiService.SenzingEngineConfigurationJson, restApiService.SenzingVerboseLogging)
			if err != nil {
				panic(err)
			}
		}
	})
	return restApiService.g2configSingleton
}

// Singleton pattern for g2config.
// See https://medium.com/golang-issue/how-singleton-pattern-works-with-golang-2fdd61cd5a7f
func (restApiService *RestApiServiceImpl) getG2configmgr(ctx context.Context) g2api.G2configmgr {
	var err error = nil
	restApiService.g2configmgrSyncOnce.Do(func() {
		restApiService.g2configmgrSingleton, err = restApiService.getAbstractFactory().GetG2configmgr(ctx)
		if err != nil {
			panic(err)
		}
		if restApiService.g2configmgrSingleton.GetSdkId(ctx) == factory.ImplementedByBase {
			err = restApiService.g2configmgrSingleton.Init(ctx, restApiService.SenzingModuleName, restApiService.SenzingEngineConfigurationJson, restApiService.SenzingVerboseLogging)
			if err != nil {
				panic(err)
			}
		}
	})
	return restApiService.g2configmgrSingleton
}

// --- Misc -------------------------------------------------------------------

func (restApiService *RestApiServiceImpl) getOptSzLinks() api.OptSzLinks {
	var result api.OptSzLinks
	szLinks := api.SzLinks{
		Self:                 api.NewOptString("SelfBob"),
		OpenApiSpecification: api.NewOptString("OpenApiSpecificationBob"),
	}
	result = api.NewOptSzLinks(szLinks)
	return result
}

func (restApiService *RestApiServiceImpl) getOptSzMeta() api.OptSzMeta {
	var result api.OptSzMeta
	szMeta := api.SzMeta{
		Server:                     api.NewOptString("ServerBob"),
		HttpMethod:                 api.NewOptSzHttpMethod(api.SzHttpMethodGET),
		HttpStatusCode:             api.NewOptInt(200),
		Timestamp:                  api.NewOptDateTime(time.Now()),
		Version:                    api.NewOptString("VersionBob"),
		RestApiVersion:             api.NewOptString("RestApiVersionBob"),
		NativeApiVersion:           api.NewOptString("NativeApiVersionBob"),
		NativeApiBuildVersion:      api.NewOptString("NativeApiBuildVersionBob"),
		NativeApiBuildNumber:       api.NewOptString("NativeApiBuildNumberBob"),
		NativeApiBuildDate:         api.NewOptDateTime(time.Now()),
		ConfigCompatibilityVersion: api.NewOptString("ConfigCompatibilityVersionBob"),
		Timings:                    api.NewOptNilSzMetaTimings(map[string]int64{}),
	}
	result = api.NewOptSzMeta(szMeta)
	return result
}

// ----------------------------------------------------------------------------
// Interface methods
// See https://github.com/docktermj/go-rest-api-client/blob/main/senzingrestpapi/oas_unimplemented_gen.go
// ----------------------------------------------------------------------------

func (restApiService *RestApiServiceImpl) AddDataSources(ctx context.Context, req api.AddDataSourcesReq, params api.AddDataSourcesParams) (r api.AddDataSourcesRes, _ error) {
	var err error = nil
	if restApiService.isTrace {
		entryTime := time.Now()
		restApiService.traceEntry(99)
		defer func() { restApiService.traceExit(99, err, time.Since(entryTime)) }()
	}

	// URL parameters.

	dataSource := params.DataSource
	withRaw := params.WithRaw

	fmt.Printf(">>>>>> %+v\n", params)
	fmt.Printf(">>>>>> type: %s   value: %v\n", reflect.TypeOf(params.DataSource), params.DataSource)

	// Get Senzing resources.

	g2Config := restApiService.getG2config(ctx)
	g2Configmgr := restApiService.getG2configmgr(ctx)

	// Get an in-memory version of the existing Senzing configuration.

	configID, err := g2Configmgr.GetDefaultConfigID(ctx)
	if err != nil {
		restApiService.log(9999, dataSource, withRaw, err)
	}

	configurationString, err := g2Configmgr.GetConfig(ctx, configID)
	if err != nil {
		restApiService.log(9999, dataSource, withRaw, err)
	}

	configurationHandle, err := g2Config.Load(ctx, configurationString)
	if err != nil {
		restApiService.log(9999, dataSource, withRaw, err)
	}

	// Add DataSouces to in-memory version of Senzing Configuration.

	sdkResponses := []string{}
	for _, dataSource := range params.DataSource {
		sdkRequest := fmt.Sprintf(`{"DSRC_CODE": "%s"}`, dataSource)

		fmt.Printf(">>>>>> sdkRequest: %s; configurationHandle: %v\n", sdkRequest, configurationHandle)

		sdkResponse, err := g2Config.AddDataSource(ctx, configurationHandle, sdkRequest)
		if err != nil {
			return r, err
		}
		sdkResponses = append(sdkResponses, sdkResponse)
	}

	// Persist in-memory Senzing Configuration to Senzing database SYS_CFG table.

	newConfigurationString, err := g2Config.Save(ctx, configurationHandle)
	if err != nil {
		restApiService.log(9999, dataSource, withRaw, err)
	}
	newConfigId, err := g2Configmgr.AddConfig(ctx, newConfigurationString, "FIXME: description")
	if err != nil {
		restApiService.log(9999, dataSource, withRaw, err)
	}
	err = g2Configmgr.SetDefaultConfigID(ctx, newConfigId)
	if err != nil {
		restApiService.log(9999, dataSource, withRaw, err)
	}

	// Retrieve all DataSources

	rawData, err := g2Config.ListDataSources(ctx, configurationHandle)
	if err != nil {
		return r, err
	}

	fmt.Printf(">>>>>> ListDataSources: %s\n", rawData)

	err = g2Config.Close(ctx, configurationHandle)

	fmt.Println(sdkResponses)

	// type SzDataSource struct {
	// 	// The data source code.
	// 	DataSourceCode OptString `json:"dataSourceCode"`
	// 	// The data source ID. The value can be null when used for input in creating a data source to
	// 	// indicate that the data source ID should be auto-generated.
	// 	DataSourceId OptNilInt32 `json:"dataSourceId"`
	// }

	szDataSource := &api.SzDataSource{
		DataSourceCode: api.NewOptString("DataSourceCodeBob"),
		DataSourceId:   api.NewOptNilInt32(1),
	}

	// type SzDataSourcesResponseDataDataSourceDetails map[string]SzDataSource

	szDataSourcesResponseDataDataSourceDetails := &api.SzDataSourcesResponseDataDataSourceDetails{
		"xxxBob": *szDataSource,
	}

	// type OptSzDataSourcesResponseDataDataSourceDetails struct {
	// 	Value SzDataSourcesResponseDataDataSourceDetails
	// 	Set   bool
	// }

	optSzDataSourcesResponseDataDataSourceDetails := &api.OptSzDataSourcesResponseDataDataSourceDetails{
		Value: *szDataSourcesResponseDataDataSourceDetails,
		Set:   true,
	}

	// type SzDataSourcesResponseData struct {
	// 	// The list of data source codes for the configured data sources.
	// 	DataSources []string `json:"dataSources"`
	// 	// The list of `SzDataSource` instances describing the data sources that are configured.
	// 	DataSourceDetails OptSzDataSourcesResponseDataDataSourceDetails `json:"dataSourceDetails"`
	// }

	szDataSourcesResponseData := &api.SzDataSourcesResponseData{
		DataSources:       []string{"Bobber"},
		DataSourceDetails: *optSzDataSourcesResponseDataDataSourceDetails,
	}

	// type OptSzDataSourcesResponseData struct {
	// 	Value SzDataSourcesResponseData
	// 	Set   bool
	// }

	optSzDataSourcesResponseData := &api.OptSzDataSourcesResponseData{
		Value: *szDataSourcesResponseData,
		Set:   true,
	}

	// type SzDataSourcesResponse struct {
	// 	Data OptSzDataSourcesResponseData `json:"data"`
	// }

	r = &api.SzDataSourcesResponse{
		Data: *optSzDataSourcesResponseData,
	}

	// Condensed version of "r"

	r = &api.SzDataSourcesResponse{
		Data: api.OptSzDataSourcesResponseData{
			Set: true,
			Value: api.SzDataSourcesResponseData{
				DataSources: []string{"Bobber"},
				DataSourceDetails: api.OptSzDataSourcesResponseDataDataSourceDetails{
					Set: true,
					Value: api.SzDataSourcesResponseDataDataSourceDetails{
						"xxxBob": api.SzDataSource{
							DataSourceCode: api.NewOptString("BOBBER5"),
							DataSourceId:   api.NewOptNilInt32(1),
						},
					},
				},
			},
		},
	}

	return r, err
}

func (restApiService *RestApiServiceImpl) Heartbeat(ctx context.Context) (r *api.SzBaseResponse, _ error) {
	var err error = nil
	r = &api.SzBaseResponse{
		Links: restApiService.getOptSzLinks(),
		Meta:  restApiService.getOptSzMeta(),
	}
	return r, err
}
