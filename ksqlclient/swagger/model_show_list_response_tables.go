/*
 * KSQL
 *
 * This is a swagger spec for ksqldb
 *
 * API version: 1.0.0
 * Generated by: Swagger Codegen (https://github.com/swagger-api/swagger-codegen.git)
 */
package swagger

type ShowListResponseTables struct {
	Name       string  `json:"name,omitempty"`
	Topic      string  `json:"topic,omitempty"`
	Format     *Format `json:"format,omitempty"`
	Type_      string  `json:"type,omitempty"`
	IsWindowed bool    `json:"isWindowed,omitempty"`
}