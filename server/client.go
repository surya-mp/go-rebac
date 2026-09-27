package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/surya-mp/go-rebac"
)

const maxResponseBytes = 1 << 20

// Client is a small typed client for Server's versioned HTTP endpoints.
// Authentication remains the host application's responsibility; configure it
// on the supplied http.Client transport.
type Client struct {
	baseURL string
	http    *http.Client
}

// NewClient constructs a client for a Server base URL. A nil HTTP client uses
// http.DefaultClient.
func NewClient(baseURL string, client *http.Client) (*Client, error) {
	parsed, err := url.ParseRequestURI(baseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, errors.New("rebac/server: absolute base URL is required")
	}
	if client == nil {
		client = http.DefaultClient
	}
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), http: client}, nil
}

// ResponseError reports a non-successful HTTP response.
type ResponseError struct {
	StatusCode int
	Message    string
}

func (e *ResponseError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("rebac/server: HTTP %d", e.StatusCode)
	}
	return fmt.Sprintf("rebac/server: HTTP %d: %s", e.StatusCode, e.Message)
}

func (c *Client) post(ctx context.Context, path string, request, response any) error {
	if c == nil || c.http == nil {
		return errors.New("rebac/server: client is nil")
	}
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpResponse, err := c.http.Do(httpRequest)
	if err != nil {
		return err
	}
	defer httpResponse.Body.Close()
	limited := io.LimitReader(httpResponse.Body, maxResponseBytes)
	if httpResponse.StatusCode < http.StatusOK || httpResponse.StatusCode >= http.StatusMultipleChoices {
		var failure struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(limited).Decode(&failure)
		return &ResponseError{StatusCode: httpResponse.StatusCode, Message: failure.Error}
	}
	if response == nil {
		return nil
	}
	return json.NewDecoder(limited).Decode(response)
}

func (c *Client) Check(ctx context.Context, request CheckRequest) (CheckResponse, error) {
	var response CheckResponse
	err := c.post(ctx, "/v1/check", request, &response)
	return response, err
}

func (c *Client) BatchCheck(ctx context.Context, request BatchCheckRequest) (BatchCheckResponse, error) {
	var response BatchCheckResponse
	err := c.post(ctx, "/v1/batch-check", request, &response)
	return response, err
}

func (c *Client) ReadTuples(ctx context.Context, request rebac.ReadTuplesRequest) (rebac.TuplePage, error) {
	var response rebac.TuplePage
	err := c.post(ctx, "/v1/tuples/read", request, &response)
	return response, err
}

func (c *Client) LookupResources(ctx context.Context, request rebac.LookupResourcesRequest) (rebac.ResourcePage, error) {
	var response rebac.ResourcePage
	err := c.post(ctx, "/v1/lookup/resources", request, &response)
	return response, err
}

func (c *Client) LookupSubjects(ctx context.Context, request rebac.LookupSubjectsRequest) (rebac.SubjectPage, error) {
	var response rebac.SubjectPage
	err := c.post(ctx, "/v1/lookup/subjects", request, &response)
	return response, err
}

func (c *Client) Expand(ctx context.Context, request ExpandRequest) (ExpandResponse, error) {
	var response ExpandResponse
	err := c.post(ctx, "/v1/expand", request, &response)
	return response, err
}

func (c *Client) WriteTuple(ctx context.Context, tuple rebac.RelationTuple) (rebac.Revision, error) {
	var response revisionResponse
	err := c.post(ctx, "/v1/tuples/write", tupleRequest{Tuple: tuple}, &response)
	return response.Revision, err
}

func (c *Client) DeleteTuple(ctx context.Context, tuple rebac.RelationTuple) (rebac.Revision, error) {
	var response revisionResponse
	err := c.post(ctx, "/v1/tuples/delete", tupleRequest{Tuple: tuple}, &response)
	return response.Revision, err
}

func (c *Client) Mutate(ctx context.Context, changes []rebac.TupleChange, preconditions []rebac.Precondition) (rebac.Revision, error) {
	var response revisionResponse
	err := c.post(ctx, "/v1/tuples/mutate", mutateRequest{Changes: changes, Preconditions: preconditions}, &response)
	return response.Revision, err
}

func (c *Client) DeleteObject(ctx context.Context, tenantID, namespace, objectID string) (rebac.Revision, error) {
	var response revisionResponse
	err := c.post(ctx, "/v1/objects/delete", objectRequest{TenantID: tenantID, Namespace: namespace, ObjectID: objectID}, &response)
	return response.Revision, err
}

func (c *Client) ReadModel(ctx context.Context, request ReadModelRequest) (rebac.ModelDocument, error) {
	var response rebac.ModelDocument
	err := c.post(ctx, "/v1/models/read", request, &response)
	return response, err
}

func (c *Client) ListModelVersions(ctx context.Context, request ReadModelRequest) ([]rebac.ModelDocument, error) {
	var response []rebac.ModelDocument
	err := c.post(ctx, "/v1/models/versions", request, &response)
	return response, err
}

func (c *Client) ReadActiveModel(ctx context.Context, request ReadModelRequest) (rebac.ModelDocument, error) {
	var response rebac.ModelDocument
	err := c.post(ctx, "/v1/models/active", request, &response)
	return response, err
}

func (c *Client) WriteModel(ctx context.Context, request WriteModelRequest) (rebac.ModelDocument, error) {
	var response rebac.ModelDocument
	err := c.post(ctx, "/v1/models/write", request, &response)
	return response, err
}

func (c *Client) ActivateModel(ctx context.Context, request ActivateModelRequest) (ActivateModelResponse, error) {
	var response ActivateModelResponse
	err := c.post(ctx, "/v1/models/activate", request, &response)
	return response, err
}
