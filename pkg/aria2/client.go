package aria2

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"time"
)

type Client struct {
	url    string
	token  string
	client *http.Client
}

type JSONRPCRequest struct {
	ID      string        `json:"id"`
	JSONRPC string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

type JSONRPCResponse struct {
	ID      string          `json:"id"`
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result"`
	Error   *RPCError       `json:"error,omitempty"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type DownloadStatus struct {
	GID             string `json:"gid"`
	Status          string `json:"status"`
	TotalLength     string `json:"totalLength"`
	CompletedLength string `json:"completedLength"`
	DownloadSpeed   string `json:"downloadSpeed"`
	ErrorCode       string `json:"errorCode,omitempty"`
	ErrorMessage    string `json:"errorMessage,omitempty"`
}

type DownloadOptions struct {
	Out              string `json:"out"`
	Dir              string `json:"dir"`
	CheckCertificate string `json:"check-certificate"`
}

func NewClient(url, token string) *Client {
	return &Client{
		url:   url,
		token: token,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *Client) call(method string, params []interface{}) (json.RawMessage, error) {
	reqID := fmt.Sprintf("%.16f", rand.Float64())
	
	req := JSONRPCRequest{
		ID:      reqID,
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
	}

	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequest("POST", c.url, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json, text/plain, */*")

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	var rpcResp JSONRPCResponse
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("rpc error: %s (code: %d)", rpcResp.Error.Message, rpcResp.Error.Code)
	}

	return rpcResp.Result, nil
}

func (c *Client) AddDownload(url string, options DownloadOptions) (string, error) {
	params := []interface{}{
		"token:" + c.token,
		[]string{url},
		options,
	}

	result, err := c.call("aria2.addUri", params)
	if err != nil {
		return "", err
	}

	var gid string
	if err := json.Unmarshal(result, &gid); err != nil {
		return "", fmt.Errorf("unmarshal gid: %w", err)
	}

	return gid, nil
}

func (c *Client) GetStatus(gid string) (*DownloadStatus, error) {
	params := []interface{}{
		"token:" + c.token,
		gid,
	}

	result, err := c.call("aria2.tellStatus", params)
	if err != nil {
		return nil, err
	}

	var status DownloadStatus
	if err := json.Unmarshal(result, &status); err != nil {
		return nil, fmt.Errorf("unmarshal status: %w", err)
	}

	return &status, nil
}

func (c *Client) RemoveDownloadResult(gid string) error {
	params := []interface{}{
		"token:" + c.token,
		gid,
	}

	_, err := c.call("aria2.removeDownloadResult", params)
	return err
}

func (c *Client) WaitForCompletion(gid string, timeout time.Duration) error {
	start := time.Now()
	lastProgress := ""

	for {
		if time.Since(start) > timeout {
			return fmt.Errorf("timeout after %v", timeout)
		}

		status, err := c.GetStatus(gid)
		if err != nil {
			return fmt.Errorf("get status: %w", err)
		}

		switch status.Status {
		case "complete":
			return nil
		case "error":
			msg := status.ErrorMessage
			if msg == "" {
				msg = fmt.Sprintf("error code: %s", status.ErrorCode)
			}
			return fmt.Errorf("download failed: %s", msg)
		case "removed":
			return fmt.Errorf("download was removed")
		case "active", "waiting", "paused":
			progress := c.formatProgress(status)
			if progress != lastProgress {
				fmt.Printf("\r%s", progress)
				lastProgress = progress
			}
			time.Sleep(3 * time.Second)
		default:
			return fmt.Errorf("unknown status: %s", status.Status)
		}
	}
}

func (c *Client) formatProgress(status *DownloadStatus) string {
	var total, completed, speed int64
	fmt.Sscanf(status.TotalLength, "%d", &total)
	fmt.Sscanf(status.CompletedLength, "%d", &completed)
	fmt.Sscanf(status.DownloadSpeed, "%d", &speed)

	if total == 0 {
		return fmt.Sprintf("[下载] 等待中... 速度: %.2fMB/s", float64(speed)/1024/1024)
	}

	progress := float64(completed) / float64(total) * 100
	totalMB := float64(total) / 1024 / 1024
	completedMB := float64(completed) / 1024 / 1024
	speedMB := float64(speed) / 1024 / 1024

	return fmt.Sprintf("[下载] %.1f%% (%.1fMB/%.1fMB, 速度: %.2fMB/s)    ",
		progress, completedMB, totalMB, speedMB)
}
