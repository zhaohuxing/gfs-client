package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"gopkg.in/yaml.v2"
)

type StorageClusterSettings struct {
	Name         string   `yaml:"name"`
	Hosts        []string `yaml:"hosts"`
	Volume       string   `yaml:"volume"`
	StorageClass string   `yaml:"storage_class"`
	ReadOnly     bool     `yaml:"readonly"`
}

type GlusterFsSettings struct {
	Clusters                  []*StorageClusterSettings `yaml:"clusters"`
	WorkerPerCPU              int                       `yaml:"worker_per_cpu"`
	BlockUsageThreshold       int32                     `yaml:"block_usage_threshold"`
	InodeUsageThreshold       int32                     `yaml:"inode_usage_threshold"`
	UpdateUsageInterval       int                       `yaml:"update_usage_interval"`
	MaxRetries                int                       `yaml:"max_retries"` // Max retries for mount glusterfs, Infinite retry if set <= 0
	TruckAlignmentSize        uint32                    `yaml:"truck_alignment_size"`
	TruckOptimizationEnabled  bool                      `yaml:"truck_optimization_enabled"`
	TruckVersion              uint8                     `yaml:"-"`
	VerifyTruckSize           bool                      `yaml:"verify_truck_size"`
	ReadonlyCPUNumberReadonly uint32                    `yaml:"readonly_cpu_number"`
}

type SettingsType struct {
	Gluster *GlusterFsSettings `yaml:"glusterfs"`
}

func ParseConf(content []byte) *SettingsType {
	settings := &SettingsType{}

	err := yaml.Unmarshal(content, &settings)
	if err != nil {
		fmt.Printf("cannot parse config: %v\n", err)
		os.Exit(1)
	}

	if len(settings.Gluster.Clusters) == 0 {
		fmt.Println("At least one cluster must be specified")
		os.Exit(1)
	}

	for i := 0; i < len(settings.Gluster.Clusters); i++ {
		if settings.Gluster.Clusters[i].Name == "" {
			settings.Gluster.Clusters[i].Name = fmt.Sprintf("cluster%d", i)
		}
	}

	return settings
}
func main() {
	var filename string
	flag.StringVar(&filename, "f", "default value", "The truck file")
	flag.Parse()

	confFile := "/qingstor/etc/qs_gateway2.yaml"
	configContent, err := ioutil.ReadFile(confFile)
	if err != nil {
		fmt.Printf("Read config file %s fail: %v", confFile, err)
		os.Exit(1)
	}

	settings := ParseConf(configContent)
	fmt.Println(time.Now(), "load config", confFile)
	fmt.Printf("settings: %v", settings)
}
