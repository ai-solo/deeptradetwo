package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	ecs "github.com/alibabacloud-go/ecs-20140526/v4/client"
	openapi "github.com/alibabacloud-go/darabonba-openapi/v2/client"
	"github.com/alibabacloud-go/tea/tea"
	"github.com/BurntSushi/toml"
)

// ---- 配置结构 ----

type Config struct {
	AccessKeyID     string    `toml:"access_key_id"`
	AccessKeySecret string    `toml:"access_key_secret"`
	Region          string    `toml:"region"`
	Year            int       `toml:"year"`
	Months          []int     `toml:"months"`
	ECS             ECSConf   `toml:"ecs"`
	Git             GitConf   `toml:"git"`
	Aria2           Aria2Conf `toml:"aria2"`
	FileServer      FSConf    `toml:"fileserver"`
	OSS             OSSConf   `toml:"oss"`
	MySQL           MySQLConf `toml:"mysql"`
	Workers         int       `toml:"workers"`
}

type ECSConf struct {
	InstanceType        string  `toml:"instance_type"`
	SystemDiskSize      int     `toml:"system_disk_size"`
	DataDiskSize        int     `toml:"data_disk_size"`
	ImageID             string  `toml:"image_id"`
	SecurityGroupID     string  `toml:"security_group_id"`
	VSwitchID           string  `toml:"vswitch_id"`
	NamePrefix          string  `toml:"name_prefix"`
	InstanceChargeType  string  `toml:"instance_charge_type"`
	ReadyTimeoutSec     int     `toml:"ready_timeout_sec"`
	RAMRoleName         string  `toml:"ram_role_name"`
	// 抢占式实例
	SpotStrategy        string  `toml:"spot_strategy"`   // SpotAsPriceGo | SpotWithPriceLimit | NoSpot
	SpotPriceLimit      float32 `toml:"spot_price_limit"` // 出价上限(元/小时)，SpotWithPriceLimit 时有效
}

type GitConf struct {
	Repo   string `toml:"repo"`
	Branch string `toml:"branch"`
}

type Aria2Conf struct {
	Token string `toml:"token"`
}

type FSConf struct {
	URL      string `toml:"url"`
	Auth     string `toml:"auth"`
	Username string `toml:"username"`
	PassHash string `toml:"pass_hash"`
}

type OSSConf struct {
	AccessKey string `toml:"access_key"`
	SecretKey string `toml:"secret_key"`
	Endpoint  string `toml:"endpoint"`
	Bucket    string `toml:"bucket"`
	Path      string `toml:"path"`
}

type MySQLConf struct {
	Host     string `toml:"host"`
	Port     int    `toml:"port"`
	User     string `toml:"user"`
	Password string `toml:"password"`
	Database string `toml:"database"`
}


// ---- 主逻辑 ----

func main() {
	configFile := flag.String("config", "pipeline.toml", "配置文件路径")
	dryRun := flag.Bool("dry-run", false, "仅打印 UserData，不实际创建 ECS")
	monthsFlag := flag.String("months", "", "覆盖配置中的月份，逗号分隔，如 1,2,3")
	flag.Parse()

	var cfg Config
	if _, err := toml.DecodeFile(*configFile, &cfg); err != nil {
		log.Fatalf("读取配置失败: %v", err)
	}

	// 环境变量覆盖 AK/SK
	if v := os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_ID"); v != "" {
		cfg.AccessKeyID = v
	}
	if v := os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET"); v != "" {
		cfg.AccessKeySecret = v
	}

	// 确定要处理的月份
	months := cfg.Months
	if *monthsFlag != "" {
		months = nil
		for _, s := range strings.Split(*monthsFlag, ",") {
			var m int
			fmt.Sscanf(strings.TrimSpace(s), "%d", &m)
			if m >= 1 && m <= 12 {
				months = append(months, m)
			}
		}
	}
	if len(months) == 0 {
		for m := 1; m <= 12; m++ {
			months = append(months, m)
		}
	}

	log.Printf("[pipeline] 年份: %d, 月份: %v", cfg.Year, months)
	log.Printf("[pipeline] 共 %d 台 ECS", len(months))

	if *dryRun {
		for _, m := range months {
			ud := buildUserData(cfg, m)
			fmt.Printf("\n===== UserData: %d-%02d =====\n%s\n", cfg.Year, m, ud)
		}
		return
	}

	// 校验必要参数
	if cfg.AccessKeyID == "" || cfg.AccessKeySecret == "" {
		log.Fatal("缺少阿里云 AK/SK，请在配置文件或环境变量 ALIBABA_CLOUD_ACCESS_KEY_ID / ALIBABA_CLOUD_ACCESS_KEY_SECRET 中设置")
	}
	if cfg.ECS.SecurityGroupID == "" || cfg.ECS.VSwitchID == "" {
		log.Fatal("缺少 ecs.security_group_id 或 ecs.vswitch_id")
	}
	if cfg.Git.Repo == "" {
		log.Fatal("缺少 git.repo")
	}
	if cfg.FileServer.URL == "" {
		log.Fatal("缺少 fileserver.url")
	}

	// 创建 ECS 客户端
	ecsClient, err := newECSClient(cfg)
	if err != nil {
		log.Fatalf("创建 ECS 客户端失败: %v", err)
	}

	// 并发为每个月创建一台 ECS
	type result struct {
		month      int
		instanceID string
		err        error
	}
	results := make([]result, len(months))
	var wg sync.WaitGroup

	for i, m := range months {
		wg.Add(1)
		go func(idx, month int) {
			defer wg.Done()
			id, err := createECS(ecsClient, cfg, month)
			results[idx] = result{month: month, instanceID: id, err: err}
		}(i, m)
	}
	wg.Wait()

	// 打印结果
	log.Println("\n========== 创建结果 ==========")
	succ, fail := 0, 0
	for _, r := range results {
		if r.err != nil {
			log.Printf("  [失败] %d-%02d: %v", cfg.Year, r.month, r.err)
			fail++
		} else {
			log.Printf("  [成功] %d-%02d => %s", cfg.Year, r.month, r.instanceID)
			succ++
		}
	}
	log.Printf("成功: %d, 失败: %d", succ, fail)
	log.Println("ECS 实例已启动，将在数据处理完成后自动释放。")
}

func newECSClient(cfg Config) (*ecs.Client, error) {
	config := &openapi.Config{
		AccessKeyId:     tea.String(cfg.AccessKeyID),
		AccessKeySecret: tea.String(cfg.AccessKeySecret),
		RegionId:        tea.String(cfg.Region),
		Endpoint:        tea.String(fmt.Sprintf("ecs.%s.aliyuncs.com", cfg.Region)),
	}
	return ecs.NewClient(config)
}

func createECS(client *ecs.Client, cfg Config, month int) (string, error) {
	instanceName := fmt.Sprintf("%s-%d-%02d", cfg.ECS.NamePrefix, cfg.Year, month)
	userData := buildUserData(cfg, month)
	userDataB64 := base64.StdEncoding.EncodeToString([]byte(userData))

	systemDisk := &ecs.RunInstancesRequestSystemDisk{
		Size:     tea.String(fmt.Sprintf("%d", cfg.ECS.SystemDiskSize)),
		Category: tea.String("cloud_essd"),
	}

	req := &ecs.RunInstancesRequest{
		RegionId:           tea.String(cfg.Region),
		ImageId:            tea.String(cfg.ECS.ImageID),
		InstanceType:       tea.String(cfg.ECS.InstanceType),
		SecurityGroupId:    tea.String(cfg.ECS.SecurityGroupID),
		VSwitchId:          tea.String(cfg.ECS.VSwitchID),
		InstanceName:       tea.String(instanceName),
		HostName:           tea.String(instanceName),
		InstanceChargeType: tea.String(cfg.ECS.InstanceChargeType),
		SystemDisk:         systemDisk,
		UserData:           tea.String(userDataB64),
		Amount:             tea.Int32(1),
		MinAmount:          tea.Int32(1),
	}

	// 数据盘
	if cfg.ECS.DataDiskSize > 0 {
		req.DataDisk = []*ecs.RunInstancesRequestDataDisk{
			{
				Size:     tea.Int32(int32(cfg.ECS.DataDiskSize)),
				Category: tea.String("cloud_essd"),
				DeleteWithInstance: tea.Bool(true),
			},
		}
	}

	if cfg.ECS.RAMRoleName != "" {
		req.RamRoleName = tea.String(cfg.ECS.RAMRoleName)
	}

	// 抢占式实例
	if cfg.ECS.SpotStrategy != "" {
		req.SpotStrategy = tea.String(cfg.ECS.SpotStrategy)
		if cfg.ECS.SpotStrategy == "SpotWithPriceLimit" && cfg.ECS.SpotPriceLimit > 0 {
			req.SpotPriceLimit = tea.Float32(cfg.ECS.SpotPriceLimit)
		}
	}

	log.Printf("[%d-%02d] 创建 ECS: %s (规格: %s, 抢占策略: %s)", cfg.Year, month, instanceName, cfg.ECS.InstanceType, cfg.ECS.SpotStrategy)
	resp, err := client.RunInstances(req)
	if err != nil {
		return "", fmt.Errorf("RunInstances 失败: %w", err)
	}
	if len(resp.Body.InstanceIdSets.InstanceIdSet) == 0 {
		return "", fmt.Errorf("RunInstances 返回空实例列表")
	}
	instanceID := tea.StringValue(resp.Body.InstanceIdSets.InstanceIdSet[0])
	log.Printf("[%d-%02d] 实例创建成功: %s，等待运行中...", cfg.Year, month, instanceID)

	// 等待实例变为 Running
	if err := waitRunning(client, cfg, instanceID); err != nil {
		log.Printf("[%d-%02d] 等待超时（实例仍会继续运行）: %v", cfg.Year, month, err)
	}
	return instanceID, nil
}

func waitRunning(client *ecs.Client, cfg Config, instanceID string) error {
	timeout := time.Duration(cfg.ECS.ReadyTimeoutSec) * time.Second
	if timeout == 0 {
		timeout = 5 * time.Minute
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.DescribeInstanceStatus(&ecs.DescribeInstanceStatusRequest{
			RegionId:    tea.String(cfg.Region),
			InstanceId:  []*string{tea.String(instanceID)},
		})
		if err != nil {
			return err
		}
		if len(resp.Body.InstanceStatuses.InstanceStatus) > 0 {
			status := tea.StringValue(resp.Body.InstanceStatuses.InstanceStatus[0].Status)
			if status == "Running" {
				log.Printf("  [就绪] %s 已进入 Running 状态", instanceID)
				return nil
			}
		}
		time.Sleep(10 * time.Second)
	}
	return fmt.Errorf("等待 %s Running 超时", instanceID)
}

// buildUserData 读取 userdata.sh 模板并替换占位符
func buildUserData(cfg Config, month int) string {
	// 读取同目录下的 userdata.sh
	tmplPath := "userdata.sh"
	data, err := os.ReadFile(tmplPath)
	if err != nil {
		log.Fatalf("读取 userdata.sh 失败: %v", err)
	}
	s := string(data)

	// 获取实例 ID 占位符（创建时尚未知道，由 ECS 内部通过元数据服务自获取）
	// UserData 里 ECS_INSTANCE_ID 改为运行时自获取
	replacer := strings.NewReplacer(
		"{{YEAR}}", fmt.Sprintf("%d", cfg.Year),
		"{{MONTH}}", fmt.Sprintf("%d", month),
		"{{GIT_REPO}}", cfg.Git.Repo,
		"{{GIT_BRANCH}}", cfg.Git.Branch,
		"{{ARIA2_TOKEN}}", cfg.Aria2.Token,
		"{{FILE_SERVER_URL}}", cfg.FileServer.URL,
		"{{FILE_SERVER_AUTH}}", cfg.FileServer.Auth,
		"{{FILE_SERVER_USER}}", cfg.FileServer.Username,
		"{{FILE_SERVER_PASS_HASH}}", cfg.FileServer.PassHash,
		"{{OSS_ACCESS_KEY}}", cfg.OSS.AccessKey,
		"{{OSS_SECRET_KEY}}", cfg.OSS.SecretKey,
		"{{OSS_ENDPOINT}}", cfg.OSS.Endpoint,
		"{{OSS_BUCKET}}", cfg.OSS.Bucket,
		"{{OSS_PATH}}", cfg.OSS.Path,
		"{{MYSQL_HOST}}", cfg.MySQL.Host,
		"{{MYSQL_PORT}}", fmt.Sprintf("%d", cfg.MySQL.Port),
		"{{MYSQL_USER}}", cfg.MySQL.User,
		"{{MYSQL_PASSWORD}}", cfg.MySQL.Password,
		"{{MYSQL_DATABASE}}", cfg.MySQL.Database,
		"{{WORKERS}}", fmt.Sprintf("%d", cfg.Workers),
		"{{ECS_REGION}}", cfg.Region,
		"{{ACCESS_KEY_ID}}", cfg.AccessKeyID,
		"{{ACCESS_KEY_SECRET}}", cfg.AccessKeySecret,
		// ECS_INSTANCE_ID 由 userdata.sh 运行时通过元数据服务自获取
		"{{ECS_INSTANCE_ID}}", "$(curl -sf http://100.100.100.200/latest/meta-data/instance-id || echo '')",
	)
	return replacer.Replace(s)
}
