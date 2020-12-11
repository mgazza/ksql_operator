/*
 * KSQL
 *
 * This is a swagger spec for ksqldb
 *
 * API version: 1.0.0
 * Generated by: Swagger Codegen (https://github.com/swagger-api/swagger-codegen.git)
 */
package swagger

type ShowListResponse struct {
	Tables     []ShowListResponseTables  `json:"tables,omitempty"`
	Streams    []ShowListResponseStreams `json:"streams,omitempty"`
	Queries    []ShowListResponseQueries `json:"queries,omitempty"`
	Properties *interface{}              `json:"properties,omitempty"`
}
