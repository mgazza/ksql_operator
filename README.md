# ksql operator
A kubernetes operator for ksql tables streams and running queries

# Manifests
This directory contains kubernetes resources used by this deployment

# args
| arg        | default        | comments                                                                                                      |
|------------|----------------|---------------------------------------------------------------------------------------------------------------|
| kubeConfig |                | Path to a kubeConfig. Only required if out-of-cluster.                                                        |
| master     |                | The address of the Kubernetes API server. Overrides any value in kubeConfig. Only required if out-of-cluster. |
| baseURL    | $KSQL_URL      | The Base URL of the ksql rest api.                                                                            |
| username   | $KSQL_USERNAME | The Username to use with the ksql rest api.                                                                   |
| password   | $KSQL_PASSWORD | The Password to use with the ksql rest api.                                                                   |

# env
| env           | default              | comments                                     |
|---------------|----------------------|----------------------------------------------|
|  KSQL_URL     | http://ksqldb        | The Base URL of the ksql rest api.           |
| KSQL_USERNAME |                      | The Username to use with the ksql rest api.  |
| KSQL_PASSWORD |                      | The Password to use with the ksql rest api.  |

# Build
This project is continuously integrated by github and produces a docker image
```bash 
docker pull ghcr.io/mgazza/ksql_operator:latest
```

# Generated resources
This project uses a few generated resources.
To regenerate the generated code issue the following command.
```bash
./hack/update-codegen.sh
```
This project uses go mod
you may need to execute `go mod vendor` before the above will work.