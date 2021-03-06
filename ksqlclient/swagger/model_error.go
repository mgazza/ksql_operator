/*
 * KSQL
 *
 * This is a swagger spec for ksqldb
 *
 * API version: 1.0.0
 * Generated by: Swagger Codegen (https://github.com/swagger-api/swagger-codegen.git)
 */
package swagger

type ModelError struct {
	Type_      string   `json:"@type,omitempty"`
	ErrorCode  float64  `json:"error_code,omitempty"`
	Message    string   `json:"message,omitempty"`
	StackTrace []string `json:"stackTrace,omitempty"`
}
