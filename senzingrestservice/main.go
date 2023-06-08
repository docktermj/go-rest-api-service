package senzingrestservice

import (
	_ "embed"

	"github.com/docktermj/go-rest-api-client/senzingrestapi"
)

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

// The RestService interface is...
type RestService interface {
	senzingrestapi.Handler
}

// ----------------------------------------------------------------------------
// Constants
// ----------------------------------------------------------------------------

// Identfier of the  package found messages having the format "senzing-6503xxxx".
const ComponentId = 9999

// Log message prefix.
const Prefix = "go-rest-api-service.senzingrestservice."

// ----------------------------------------------------------------------------
// Variables
// ----------------------------------------------------------------------------

// Message templates for g2config implementations.
var IdMessages = map[int]string{
	10: "Enter " + Prefix + "InitializeSenzing().",
}

// Status strings for specific messages.
var IdStatuses = map[int]string{}

//go:embed openapi.json
var OpenApiSpecificationJson []byte

//go:embed openapi.yaml
var OpenApiSpecificationYaml []byte
