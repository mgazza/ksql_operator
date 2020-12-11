package ksqlclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"ksql_operator/ksqlclient/swagger"
	"net/http"
	"net/url"
	"path"
)

type CommandStatus string

const (
	ContentType   = "application/vnd.ksql.v1+json"
	ErrUnexpected = Error("Unexpected response")

	ErrCodeNotFound = 40001
)

type Error string

func (e Error) Error() string { return string(e) }

type client struct {
	baseURL  *url.URL
	userName string
	password string
}

func New(baseUrl string, username string, password string) (*client, error) {
	url, err := url.Parse(baseUrl)
	return &client{baseURL: url, userName: username, password: password}, err
}

// execute a ksql Describe statement for @name
// result is either swagger.ModelError{} or swagger.DescribeResult{}
func (c client) Describe(ctx context.Context, name string) (interface{}, error) {
	return c.Execute(ctx, fmt.Sprintf("DESCRIBE %s;", name), &[]swagger.DescribeResultItem{})
}

// execute a ksql Explain statement for @name
// result is either swagger.ModelError{} or swagger.ExplainResult{}
func (c client) Explain(ctx context.Context, name string) (interface{}, error) {
	return c.Execute(ctx, fmt.Sprintf("EXPLAIN %s;", name), &[]swagger.DescribeResultItem{})
}

// execute a ksql CreateDropTerminate statement for @name
// result is either swagger.ModelError{} or swagger.CreateDropTerminateResponse{}
func (c client) CreateDropTerminate(ctx context.Context, sql string) (interface{}, error) {
	return c.Execute(ctx, sql, &[]swagger.CreateDropTerminateResponseItem{})
}

// execute a kql Statement
// the result is either swagger.ModelError{} or @result
func (c client) Execute(ctx context.Context, ksql string, result interface{}) (interface{}, error) {
	u, err := c.baseURL.Parse("ksql")
	if err != nil {
		return nil, err
	}

	requestBody := swagger.Statement{
		Ksql: ksql,
	}
	requestBodyJson, err := json.Marshal(requestBody)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewBuffer(requestBodyJson))
	if err != nil {
		return nil, err
	}
	if c.userName != "" {
		req.SetBasicAuth(c.userName, c.password)
	}
	req.Header.Set("Content-Type", ContentType)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == 200 {
		err := json.Unmarshal(body, result)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("error unmarshalling response, err: %v, body: '%s'", err, string(body)))
		}
		return result, nil
	}

	if resp.StatusCode == 400 {
		r := swagger.ModelError{}
		err := json.Unmarshal(body, &r)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("error unmarshalling response, err: %v, body: '%s'", err, string(body)))
		}
		return &r, nil
	}

	return nil, fmt.Errorf("unexpected response '%d' with body '%s'", resp.StatusCode, body)
}

// get the status of the commandID
func (c client) Status(tx context.Context, commandID string) (interface{}, error) {
	url, err := c.baseURL.Parse(path.Join("status", commandID))
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodGet, url.String(), nil)
	if err != nil {
		return nil, err
	}
	if c.userName != "" {
		req.SetBasicAuth(c.userName, c.password)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusOK {
		result := &swagger.StatusResponse{}
		err := json.Unmarshal(body, result)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("error unmarshalling response, err: %v, body: '%s'", err, string(body)))
		}
		return result, nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return &swagger.ModelError{
			ErrorCode: 404,
		}, nil
	}

	return nil, fmt.Errorf("%s code %d with body %s", ErrUnexpected, resp.StatusCode, string(body))
}
