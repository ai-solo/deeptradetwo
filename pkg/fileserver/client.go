package fileserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

type Client struct {
	baseURL      string
	auth         string
	username     string
	passwordHash string // SHA256 hashed password for /api/auth/login/hash
	client       *http.Client
	mu           sync.Mutex
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

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	OTPCode  string `json:"otp_code"`
}

type loginResponse struct {
	Code    int        `json:"code"`
	Message string     `json:"message"`
	Data    *loginData `json:"data"`
}

type loginData struct {
	DeviceKey string `json:"device_key"`
	Token     string `json:"token"`
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

// SetCredentials sets the login credentials for automatic token refresh.
// passwordHash must be the SHA256 hash of the plaintext password,
// matching what the /api/auth/login/hash endpoint expects.
func (c *Client) SetCredentials(username, passwordHash string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.username = username
	c.passwordHash = passwordHash
}

// Login calls /api/auth/login/hash to obtain a new token and updates c.auth.
func (c *Client) Login() error {
	c.mu.Lock()
	username := c.username
	passwordHash := c.passwordHash
	c.mu.Unlock()

	if username == "" || passwordHash == "" {
		return fmt.Errorf("no credentials configured for auto-login")
	}

	reqBody := loginRequest{
		Username: username,
		Password: passwordHash,
		OTPCode:  "",
	}
	data, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal login request: %w", err)
	}

	apiURL := c.baseURL + "/api/auth/login/hash"
	httpReq, err := http.NewRequest("POST", apiURL, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create login request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json;charset=UTF-8")
	httpReq.Header.Set("Accept", "application/json, text/plain, */*")

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("send login request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read login response: %w", err)
	}

	var loginResp loginResponse
	if err := json.Unmarshal(body, &loginResp); err != nil {
		return fmt.Errorf("unmarshal login response: %w", err)
	}

	if loginResp.Code != 200 {
		return fmt.Errorf("login failed: %s (code: %d)", loginResp.Message, loginResp.Code)
	}
	if loginResp.Data == nil || loginResp.Data.Token == "" {
		return fmt.Errorf("login succeeded but token is empty")
	}

	c.mu.Lock()
	c.auth = loginResp.Data.Token
	c.mu.Unlock()

	log.Printf("[文件服务器] 登录成功，已更新 token")
	return nil
}

func (c *Client) hasCredentials() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.username != "" && c.passwordHash != ""
}

func (c *Client) getAuth() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.auth
}

func (c *Client) ListFiles(path string) ([]FileInfo, error) {
	files, code, err := c.listFilesOnce(path)
	if err != nil && isAuthError(code) && c.hasCredentials() {
		log.Printf("[文件服务器] token 已过期，正在重新登录...")
		if loginErr := c.Login(); loginErr != nil {
			return nil, fmt.Errorf("token 过期且重新登录失败: %w", loginErr)
		}
		files, _, err = c.listFilesOnce(path)
	}
	return files, err
}

func isAuthError(code int) bool {
	return code == 401 || code == 403
}

func (c *Client) listFilesOnce(path string) ([]FileInfo, int, error) {
	reqData := ListRequest{
		Path:     path,
		Password: "",
		Page:     1,
		PerPage:  0,
		Refresh:  false,
	}

	data, err := json.Marshal(reqData)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal request: %w", err)
	}

	apiURL := c.baseURL + "/api/fs/list"
	httpReq, err := http.NewRequest("POST", apiURL, bytes.NewReader(data))
	if err != nil {
		return nil, 0, fmt.Errorf("create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json;charset=UTF-8")
	httpReq.Header.Set("Accept", "application/json, text/plain, */*")
	httpReq.Header.Set("Authorization", c.getAuth())

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, 0, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("read response: %w", err)
	}

	var listResp ListResponse
	if err := json.Unmarshal(body, &listResp); err != nil {
		return nil, 0, fmt.Errorf("unmarshal response: %w", err)
	}

	if listResp.Code != 200 {
		return nil, listResp.Code, fmt.Errorf("api error: %s (code: %d)", listResp.Message, listResp.Code)
	}

	if listResp.Data == nil || len(listResp.Data.Content) == 0 {
		return []FileInfo{}, listResp.Code, nil
	}

	return listResp.Data.Content, listResp.Code, nil
}

func (c *Client) BuildDownloadURL(filePath, sign string) string {
	// filePath: /量化/2025/2025.12/20251230/20251230_Index.csv.zip
	// downloadURL: http://baseURL/d/baidupan/量化/2025/2025.12/20251230/20251230_Index.csv.zip?sign=xxx

	// 注意：直接拼接，不编码路径（服务器需要中文路径）
	return fmt.Sprintf("%s/d/baidupan%s?sign=%s", c.baseURL, filePath, sign)
}
