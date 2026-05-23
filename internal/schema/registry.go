package schema

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type RegistryClient struct {
	baseURL    string
	httpClient *http.Client
}

type SubjectVersion struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
	ID      int    `json:"id"`
	Schema  string `json:"schema"`
}

func NewRegistryClient(baseURL string) *RegistryClient {
	return &RegistryClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (c *RegistryClient) Latest(ctx context.Context, subject string) (SubjectVersion, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/subjects/"+subject+"/versions/latest", nil)
	if err != nil {
		return SubjectVersion{}, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return SubjectVersion{}, fmt.Errorf("fetch latest schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return SubjectVersion{}, fmt.Errorf("fetch latest schema status=%d body=%s", resp.StatusCode, string(body))
	}

	var result SubjectVersion
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return SubjectVersion{}, fmt.Errorf("decode latest schema response: %w", err)
	}

	return result, nil
}

func (c *RegistryClient) Register(ctx context.Context, subject string, schemaText string) (int, error) {
	payload, err := json.Marshal(map[string]string{
		"schema": schemaText,
	})
	if err != nil {
		return 0, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/subjects/"+subject+"/versions", bytes.NewReader(payload))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("register schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("register schema status=%d body=%s", resp.StatusCode, string(body))
	}

	var result struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("decode register schema response: %w", err)
	}

	return result.ID, nil
}
