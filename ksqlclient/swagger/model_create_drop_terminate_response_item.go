/*
 * KSQL
 *
 * This is a swagger spec for ksqldb
 *
 * API version: 1.0.0
 * Generated by: Swagger Codegen (https://github.com/swagger-api/swagger-codegen.git)
 */
package swagger

type CreateDropTerminateResponseItem struct {
	StatementText         string                                    `json:"statementText,omitempty"`
	CommandId             string                                    `json:"commandId,omitempty"`
	CommandStatus         *CreateDropTerminateResponseCommandStatus `json:"commandStatus,omitempty"`
	CommandSequenceNumber int32                                     `json:"commandSequenceNumber,omitempty"`
}
