# go-rest-api-service

## Development

Instructions are at
[Ogen QuickStart](https://ogen.dev/docs/intro/).

1. Get latest version of `ogen`

    ```console
    cd ${GIT_REPOSITORY_DIR}
    go get -d github.com/ogen-go/ogen
    ```

1. View version.

    ```console
    go list -m github.com/ogen-go/ogen
    ```

1. Down latest version of
   [senzing-rest-api.yaml](https://raw.githubusercontent.com/Senzing/senzing-rest-api-specification/main/senzing-rest-api.yaml)
   to
   [restapiservice/openapi.yaml](https://github.com/docktermj/go-rest-api-service/blob/main/restapiservice/openapi.yaml).

1. Create `generate.go`

    ```console
    go generate ./...
    ```
