package fileserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	baseURL string
	auth    string
	client  *http.Client
}

type ListRequest struct {
	Path     string `json:"path"`
	Password string `json:"password"`
	Page     int    `json:"page"`
	PerPage  int    `json:"per_page"`
	Refresh  bool   `json:"refresh"`
}

type ListResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    *Data  `json:"data"`
}

type Data struct {
	Content  []FileInfo `json:"content"`
	Total    int        `json:"total"`
	Provider string     `json:"provider"`
}

type FileInfo struct {
	ID       string    `json:"id"`
	Path     string    `json:"path"`
	Name     string    `json:"name"`
	Size     int64     `json:"size"`
	IsDir    bool      `json:"is_dir"`
	Sign     string    `json:"sign"`
	Modified time.Time `json:"modified"`
	HashInfo *HashInfo `json:"hash_info,omitempty"`
}

type HashInfo struct {
	MD5 string `json:"md5"`
}

func NewClient(baseURL, auth string) *Client {
	return &Client{
		baseURL: baseURL,
		auth:    auth,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *Client) ListFiles(path string) ([]FileInfo, error) {
	reqData := ListRequest{
		Path:     path,
		Password: "",
		Page:     1,
		PerPage:  0, // 0 means all files
		Refresh:  false,
	}

	data, err := json.Marshal(reqData)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	apiURL := c.baseURL + "/api/fs/list"
	httpReq, err := http.NewRequest("POST", apiURL, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json;charset=UTF-8")
	httpReq.Header.Set("Accept", "application/json, text/plain, */*")
	httpReq.Header.Set("Authorization", c.auth)

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	var listResp ListResponse
	if err := json.Unmarshal(body, &listResp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	if listResp.Code != 200 {
		return nil, fmt.Errorf("api error: %s (code: %d)", listResp.Message, listResp.Code)
	}

	if listResp.Data == nil || len(listResp.Data.Content) == 0 {
		return []FileInfo{}, nil
	}

	return listResp.Data.Content, nil
}

func (c *Client) BuildDownloadURL(filePath, sign string) string {
	// filePath: /量化/2025/2025.12/20251230/20251230_Index.csv.zip
	// downloadURL: http://baseURL/d/baidupan/量化/2025/2025.12/20251230/20251230_Index.csv.zip?sign=xxx
	
	// 注意：直接拼接，不编码路径（服务器需要中文路径）
	return fmt.Sprintf("%s/d/baidupan%s?sign=%s", c.baseURL, filePath, sign)
}
