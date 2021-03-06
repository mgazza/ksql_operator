/*
 * KSQL
 *
 * This is a swagger spec for ksqldb
 *
 * API version: 1.0.0
 * Generated by: Swagger Codegen (https://github.com/swagger-api/swagger-codegen.git)
 */
package swagger

type ExplainResultItemQueryDescriptionFields struct {
	Name   string                                   `json:"name,omitempty"`
	Type_  string                                   `json:"type,omitempty"`
	Schema *ExplainResultItemQueryDescriptionSchema `json:"schema,omitempty"`
}
