package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"git.qingstor.dev/external/gogfapi/gfapi"
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

var ErrStaleNFSFileHandle error = errors.New("stale NFS file handle")

type StorageCluster struct {
	Clusters             []*GlusterFSClient          // Contains all storage all clusters
	StorageClassClusters map[int8][]*GlusterFSClient // Only contains readonly=False
}

type GlusterFSClient struct {
	Id   int16
	Name string
	//	logger       Logger
	gfapiLogPath string
	Hosts        []string
	Volume       string
	Vol          *gfapi.Volume
	StorageClass *StorageClass
	ReadOnly     bool
	BlockFlag    chan struct{} // The channel will be close when mount ok

	mountOk int32 // 1 means mounted and 0 means not mount yet
}

func (gluster *GlusterFSClient) String() string {
	return gluster.Name
}

func (gluster *GlusterFSClient) ToString() string {
	return fmt.Sprintf("%s(%v:%v)", gluster.Name,
		gluster.Hosts, gluster.Volume)
}

// Ready check the gluster client is loaded
func (gluster *GlusterFSClient) Ready() bool {
	v := atomic.LoadInt32(&gluster.mountOk)
	return v == 1
}

func (gluster *GlusterFSClient) Init(settings *SettingsType) (res int) {
	gluster.Vol = new(gfapi.Volume)

	// Try to init volume
	var cpus uint32
	if gluster.ReadOnly {
		cpus = settings.Gluster.ReadonlyCPUNumberReadonly
	} else {
		cpus = uint32(runtime.NumCPU())
	}
	res = gluster.Vol.Init(gluster.Hosts, gluster.Volume, cpus, settings.Gluster.WorkerPerCPU)
	if res != 0 {
		//gluster.logger.Errorf("Init %s fail, res=%v", gluster.ToString(), res)
		fmt.Errorf("Init %s fail, res=%v\n", gluster.ToString(), res)
		return res
	}

	//gluster.logger.Debugf("Init gluster %s done", gluster.ToString())
	fmt.Printf("Init gluster %s done\n", gluster.ToString())

	logFile := filepath.Join(
		gluster.gfapiLogPath,
		fmt.Sprintf("internal_gateway2_gf%s.log", gluster.Name),
	)
	gluster.Vol.SetLogging(logFile, gfapi.LogInfo)
	return 0
}

func (gluster *GlusterFSClient) Mount(settings *SettingsType) {
	var duration float64
	start := time.Now()
	var res int
	// Try to mount volume
	maxRetries := settings.Gluster.MaxRetries
	retries := 0
	for retries < maxRetries ||
		settings.Gluster.MaxRetries <= 0 {
		//gluster.logger.Debugf("Mounting %s, retries=%d, maxRetries=%d", gluster.ToString(), retries, maxRetries)
		fmt.Printf("Mounting %s, retries=%d, maxRetries=%d\n", gluster.ToString(), retries, maxRetries)
		if res = gluster.Vol.Mount(); res == 0 {
			goto DONE
		}
		//gluster.logger.Errorf("Mount %s fail, res=%v", gluster.ToString(), res)
		fmt.Errorf("Mount %s fail, res=%v\n", gluster.ToString(), res)
		time.Sleep(time.Second * 1)
		retries += 1
	}
	goto ERROR
DONE:
	atomic.StoreInt32(&gluster.mountOk, 1)
	close(gluster.BlockFlag)

	duration = time.Now().Sub(start).Seconds()
	if duration > 20 {
		//gluster.logger.Errorf("Mounted %s in %.02f sec", gluster.ToString(), duration)
		fmt.Errorf("Mounted %s in %.02f sec\n", gluster.ToString(), duration)
	} else {
		//gluster.logger.Debugf("Mounted %s in %.02f sec", gluster.ToString(), duration)
		fmt.Printf("Mounted %s in %.02f sec\n", gluster.ToString(), duration)
	}
	return
ERROR:
	//gluster.logger.Errorf("Give up retrying to mount cluster %s", gluster.ToString())
	fmt.Errorf("Give up retrying to mount cluster %s\n", gluster.ToString())
	return
}

func (gluster *GlusterFSClient) Shutdown() int {
	//BUG in vol.Unmount: Always returns non-zero presently.
	//gluster.logger.Debugf("Closing gluster client: %s", gluster.ToString())
	fmt.Printf("Closing gluster client: %s\n", gluster.ToString())
	gluster.Vol.Unmount()
	return 0
}

type StorageClass struct {
	Id   int8
	Name string
}

var StorageClassStandard = StorageClass{
	Id:   0,
	Name: "STANDARD",
}

var StorageClassStandardIA = StorageClass{
	Id:   1,
	Name: "STANDARD_IA",
}

var StorageClasses = []*StorageClass{
	&StorageClassStandard,
	&StorageClassStandardIA,
}

func GetStorageClassByName(name string) *StorageClass {
	name = strings.ToUpper(name)
	for _, cls := range StorageClasses {
		if cls.Name == name {
			return cls
		}
	}
	return nil
}

func NewStorageCluster( /*logger Logger,*/ settings *SettingsType) *StorageCluster {
	stors := new(StorageCluster)
	stors.Clusters = make([]*GlusterFSClient, 0, len(settings.Gluster.Clusters))
	stors.StorageClassClusters = make(map[int8][]*GlusterFSClient)
	for clusterId, c := range settings.Gluster.Clusters {
		cls := GetStorageClassByName(c.StorageClass)
		if cls == nil {
			panic(fmt.Errorf("invalid storage class: %s", c.StorageClass))
		}
		gluster := &GlusterFSClient{
			Id:     int16(clusterId),
			Name:   c.Name,
			Hosts:  c.Hosts,
			Volume: c.Volume,
			//logger:       logger,
			gfapiLogPath: "/qingstor/log",
			StorageClass: cls,
			ReadOnly:     c.ReadOnly,
			BlockFlag:    make(chan struct{}),
			mountOk:      -1,
		}
		gluster.Init(settings)
		stors.Clusters = append(stors.Clusters, gluster)
		if !c.ReadOnly {
			cs, _ := stors.StorageClassClusters[cls.Id]
			cs = append(cs, gluster)
			stors.StorageClassClusters[cls.Id] = cs
		}
	}

	for i := 0; i < len(stors.Clusters); i++ {
		// Async mount glusterfs
		go func(_i int) {
			stors.Clusters[_i].Mount(settings)
		}(i)
	}
	return stors
}

func (stors *StorageCluster) GetClusterById( /*logger Logger,*/ clusterId int,
) (c *GlusterFSClient, err error) {
	if clusterId >= len(stors.Clusters) || clusterId < 0 {
		//logger.Errorf("no such cluster %d", clusterId)
		fmt.Errorf("no such cluster %d\n", clusterId)
		return nil, errors.New("ErrInternalError")
	}

	if c = stors.Clusters[clusterId]; c.Ready() {
		return
	}
	// Wait for cluster mounted
	select {
	case _, _ = <-c.BlockFlag:
		// no need do anything
	case <-time.After(time.Second * 60):
		//logger.Errorf("storage cluster %d is not mounted", clusterId)
		fmt.Errorf("storage cluster %d is not mounted\n", clusterId)
		return nil, errors.New("ErrInternalError")
	}
	return
}

func (stors *StorageCluster) Shutdown() {
	for _, client := range stors.Clusters {
		client.Shutdown()
	}
}

func (stors *StorageCluster) RemoveTruckBlock( /*logger Logger,*/ clusterId int, truckPath string,
	offset uint32) (err error) {
	cluster, err := stors.GetClusterById( /*logger,*/ clusterId)
	if err != nil {
		return err
	}
	truck, err := gfapi.TruckOpenForDelete(cluster.Vol, truckPath)
	if err != nil {
		return err
	}

	defer func() {
		if e := truck.Close(); e != nil {
			//logger.Errorf("Close cluster %d truckPath %s truck file fail: %v", clusterId, truckPath, e)
			fmt.Errorf("Close cluster %d truckPath %s truck file fail: %v\n", clusterId, truckPath, e)
		}
	}()
	//logger.Debugf("Deleting truck block, cluster=<%d>, path=<%s>", clusterId, truckPath)
	fmt.Printf("Deleting truck block, cluster=<%d>, path=<%s>\n", clusterId, truckPath)
	err = truck.DeleteBlock(offset)
	if os.IsNotExist(err) {
		//logger.Debugf("truck block path=[%s], truckOffset=[%d] not exist", truckPath, offset)
		fmt.Printf("truck block path=[%s], truckOffset=[%d] not exist\n", truckPath, offset)
		return nil
	}
	if err != nil {
		//logger.Warnf("failed to delete truck block path=[%s], truckOffset=[%d]", truckPath, offset)
		fmt.Printf("failed to delete truck block path=[%s], truckOffset=[%d]\n", truckPath, offset)
	}
	return err
}
