/*
 * KSQL
 *
 * This is a swagger spec for ksqldb
 *
 * API version: 1.0.0
 * Generated by: Swagger Codegen (https://github.com/swagger-api/swagger-codegen.git)
 */
package swagger

type DescribeResultItemSourceDescription struct {
	Name         string                                      `json:"name,omitempty"`
	WindowType   string                                      `json:"windowType,omitempty"`
	ReadQueries  []DescribeResultItemSourceDescriptionQuery  `json:"readQueries,omitempty"`
	WriteQueries []DescribeResultItemSourceDescriptionQuery  `json:"writeQueries,omitempty"`
	Fields       []DescribeResultItemSourceDescriptionFields `json:"fields,omitempty"`
	Type_        string                                      `json:"type,omitempty"`
	Key          string                                      `json:"key,omitempty"`
	Timestamp    string                                      `json:"timestamp,omitempty"`
	Format       *Format                                     `json:"format,omitempty"`
	Topic        string                                      `json:"topic,omitempty"`
	Extended     bool                                        `json:"extended,omitempty"`
	Statistics   string                                      `json:"statistics,omitempty"`
	ErrorStats   string                                      `json:"errorStats,omitempty"`
	Replication  int32                                       `json:"replication,omitempty"`
	Partitions   int32                                       `json:"partitions,omitempty"`
	Statement    string                                      `json:"statement,omitempty"`
}
