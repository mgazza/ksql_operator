/*
 * KSQL
 *
 * This is a swagger spec for ksqldb
 *
 * API version: 1.0.0
 * Generated by: Swagger Codegen (https://github.com/swagger-api/swagger-codegen.git)
 */
package swagger

type DescribeResultItem struct {
	Type_             string                               `json:"@type,omitempty"`
	StatementText     string                               `json:"statementText,omitempty"`
	SourceDescription *DescribeResultItemSourceDescription `json:"sourceDescription,omitempty"`
}
