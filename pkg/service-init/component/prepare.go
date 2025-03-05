package component

import (
	"os"
	"path/filepath"

	"yunion.io/x/jsonutils"
	"yunion.io/x/log"
	"yunion.io/x/onecloud/pkg/mcclient"
	"yunion.io/x/onecloud/pkg/util/fileutils2"
	"yunion.io/x/pkg/errors"

	"yunion.io/x/onecloud-operator/pkg/apis/onecloud/v1alpha1"
	"yunion.io/x/onecloud-operator/pkg/controller"
	"yunion.io/x/onecloud-operator/pkg/manager/config"
	"yunion.io/x/onecloud-operator/pkg/service-init/cluster"
	"yunion.io/x/onecloud-operator/pkg/util/onecloud"
)

type PrepareManager interface {
	ConstructCluster(config v1alpha1.Mysql, publicIp string, cfgDir string, pv v1alpha1.ProductVersion) (*v1alpha1.OnecloudCluster, error)
	ConstructClusterConfig(cfgDir string) (*v1alpha1.OnecloudClusterConfig, error)
	Init(cpnt Component, oc *v1alpha1.OnecloudCluster, cfg *v1alpha1.OnecloudClusterConfig, targetCfgDir string) error
	PostInit(cpnt Component, oc *v1alpha1.OnecloudCluster, targetCfgDir string) error
}

type prepareManager struct{}

func NewPrepareManager() PrepareManager {
	return &prepareManager{}
}

func (m *prepareManager) ConstructCluster(dbConfig v1alpha1.Mysql, publicIp string, targetDir string, pv v1alpha1.ProductVersion) (*v1alpha1.OnecloudCluster, error) {
	cls := cluster.NewDockerComposeCluster(dbConfig, publicIp, "region0", "v3.9.8", pv)
	enableTls := true
	cls.Spec.Etcd.EnableTls = &enableTls

	if err := ConstructCluster(targetDir, cls); err != nil {
		return nil, errors.Wrap(err, "construct cluster")
	}

	return cls, nil
}

func (m *prepareManager) ConstructClusterConfig(cfgDir string) (*v1alpha1.OnecloudClusterConfig, error) {
	clusterCfg := config.NewClusterConfig()
	if err := ConstructClusterConfig(cfgDir, clusterCfg); err != nil {
		return nil, errors.Wrap(err, "construct cluster config")
	}
	return clusterCfg, nil
}

func ConstructCluster(cfgDir string, cluster *v1alpha1.OnecloudCluster) error {
	return ConstructClusterResource(cfgDir, func(cpnt Component, cpntOpt interface{}) error {
		return cpnt.BuildCluster(cluster, cpntOpt)
	})
}

func ConstructClusterConfig(cfgDir string, cfg *v1alpha1.OnecloudClusterConfig) error {
	return ConstructClusterResource(cfgDir, func(cpnt Component, cpntOpt interface{}) error {
		opt := jsonutils.Marshal(cpntOpt)
		if cpnt.GetDefaultDBConfig(cfg) != nil {
			dbCfg, err := GetComponentDBConfig(cpnt, cfg, opt)
			if err != nil {
				return errors.Wrapf(err, "Get component db config: %q", cpnt.GetType())
			}
			if err := cpnt.BuildClusterConfigDB(cfg, *dbCfg); err != nil {
				return errors.Wrapf(err, "build cluster config db: %q", cpnt.GetType())
			}
		}

		if cpnt.GetDefaultCloudUser(cfg) != nil {
			dbCfg, err := GetComponentCloudUser(cpnt, cfg, opt)
			if err != nil {
				return errors.Wrapf(err, "Get component clouduser: %q", cpnt.GetType())
			}
			if err := cpnt.BuildClusterConfigCloudUser(cfg, *dbCfg); err != nil {
				return errors.Wrapf(err, "build cluster clouduser: %q", cpnt.GetType())
			}
		}

		return nil
	})
}

func ConstructClusterResource(cfgDir string, ef func(cpnt Component, cpntOpt interface{}) error) error {
	for _, cpnt := range GetComponents() {
		cfgFilePath := getComponentConfigFilePath(cfgDir, cpnt)
		if fileutils2.Exists(cfgFilePath) {
			cpntOpt := cpnt.GetOptions()
			if cpntOpt == nil {
				log.Infof("skip read component %q config file", cpnt.GetType())
				continue
			}
			existOpt, err := readConfigOptions(cfgFilePath)
			if err != nil {
				return errors.Wrapf(err, "read config options from %q", cfgFilePath)
			}
			if err := existOpt.Unmarshal(cpntOpt); err != nil {
				return errors.Wrapf(err, "unmarshal to %q options", cpnt.GetType())
			}
			if err := ef(cpnt, cpntOpt); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *prepareManager) getComponentPhaseControl(cpnt Component, oc *v1alpha1.OnecloudCluster) controller.PhaseControl {
	cMan := NewComposeComponentManager(oc)
	return cpnt.GetPhaseControl(cMan)
}

func (m *prepareManager) Init(
	cpnt Component,
	oc *v1alpha1.OnecloudCluster,
	cfg *v1alpha1.OnecloudClusterConfig,
	targetDir string,
) error {
	log.Infof("Start init component: %q", cpnt.GetType())

	if err := m.syncCerts(oc, cpnt.GetCertsDirPath(targetDir)); err != nil {
		return errors.Wrapf(err, "sync tls certs")
	}

	cfgFilePath := getComponentConfigFilePath(targetDir, cpnt)
	var exitOpt jsonutils.JSONObject = nil
	var err error = nil
	if fileutils2.Exists(cfgFilePath) {
		exitOpt, err = readConfigOptions(cfgFilePath)
		if err != nil {
			return errors.Wrapf(err, "read config options from %q", cfgFilePath)
		}
	}

	if err := m.syncComponentDB(cpnt, oc, cfg, exitOpt); err != nil {
		return errors.Wrapf(err, "sync %q component db", cpnt.GetType())
	}

	if err := m.syncComponentConfig(cpnt, oc, cfg, targetDir); err != nil {
		return errors.Wrapf(err, "sync %q component configuration file", cpnt.GetType())
	}

	if err := m.syncCloudUser(cpnt, oc, cfg, exitOpt); err != nil {
		return errors.Wrapf(err, "sync %q component cloud user", cpnt.GetType())
	}

	if err := cpnt.BeforeStart(oc, targetDir); err != nil {
		return errors.Wrapf(err, "before component %q start", cpnt.GetType())
	}

	phaseCtrl := m.getComponentPhaseControl(cpnt, oc)
	if phaseCtrl != nil {
		if err := phaseCtrl.Setup(); err != nil {
			return errors.Wrapf(err, "setup %s component", cpnt.GetType())
		}
	}

	return nil
}

func readConfigOptions(cfgFilePath string) (jsonutils.JSONObject, error) {
	content, err := os.ReadFile(cfgFilePath)
	if err != nil {
		return nil, errors.Wrapf(err, "read content of %q", cfgFilePath)
	}
	opt, err := jsonutils.ParseYAML(string(content))
	if err != nil {
		return nil, errors.Wrapf(err, "parse yaml config of %q", cfgFilePath)
	}
	return opt, nil
}

func (m *prepareManager) syncComponentDB(
	cpnt Component,
	oc *v1alpha1.OnecloudCluster,
	cfg *v1alpha1.OnecloudClusterConfig,
	existOpt jsonutils.JSONObject,
) error {
	dbConfig, err := GetComponentDBConfig(cpnt, cfg, existOpt)
	if err != nil {
		return errors.Wrapf(err, "get db config")
	}
	if dbConfig != nil {
		// NOTE: always sync user
		controller.SyncUser = true
		if err := EnsureClusterMySQLUser(oc, *dbConfig); err != nil {
			return errors.Wrap(err, "ensure cluster mysql db user")
		}
	}

	return nil
}

func (m *prepareManager) syncCloudUser(
	cpnt Component,
	oc *v1alpha1.OnecloudCluster,
	cfg *v1alpha1.OnecloudClusterConfig,
	existOpt jsonutils.JSONObject,
) error {
	cloudUser, err := GetComponentCloudUser(cpnt, cfg, existOpt)
	if err != nil {
		return errors.Wrapf(err, "get cloud user")
	}
	if cloudUser == nil {
		return nil
	}
	if err := controller.RunWithSession(oc, func(s *mcclient.ClientSession) error {
		if err := EnsureServiceAccount(s, *cloudUser); err != nil {
			return errors.Wrapf(err, "ensure service account %#v", *cloudUser)
		}
		return nil
	}); err != nil {
		return errors.Wrap(err, "Run with session")
	}
	return nil
}

func isFileExists(targetDir string, files ...string) bool {
	if len(files) == 0 {
		return fileutils2.Exists(targetDir)
	}
	for _, f := range files {
		fp := filepath.Join(targetDir, f)
		if !fileutils2.Exists(fp) {
			return false
		}
	}
	return true
}

func (m *prepareManager) syncCerts(oc *v1alpha1.OnecloudCluster, targetDir string) error {
	if isFileExists(targetDir, "ca.crt", "ca.key", "service.crt", "service.key") {
		log.Infof("all certs file exists in dir %q, skip generate them", targetDir)
		return nil
	}

	store, err := controller.CreateCertPair(oc)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return errors.Wrapf(err, "create directory %q", targetDir)
	}
	for k, v := range store {
		tlsFile := filepath.Join(targetDir, k)
		if err := os.WriteFile(
			tlsFile,
			v,
			0644); err != nil {
			return errors.Wrapf(err, "write config file %q", tlsFile)
		}
	}
	return nil
}

func getComponentConfigFilePath(targetDir string, cpnt Component) string {
	return cpnt.GetConfigFilePath(targetDir)
}

func (m *prepareManager) syncComponentConfig(
	cpnt Component,
	oc *v1alpha1.OnecloudCluster,
	cfg *v1alpha1.OnecloudClusterConfig,
	targetDir string,
) error {
	cfgFilePath := getComponentConfigFilePath(targetDir, cpnt)
	if fileutils2.Exists(cfgFilePath) {
		log.Infof("config file %q exists, skip this step", cfgFilePath)
		return nil
	}
	opt, err := cpnt.GetConfig(oc, cfg)
	if err != nil {
		return errors.Wrapf(err, "get configuration of %q", cpnt.GetType())
	}
	if opt == nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(cfgFilePath), 0755); err != nil {
		return errors.Wrapf(err, "create directory %q", targetDir)
	}
	optContent := ""
	switch opt.(type) {
	case string:
		optContent = opt.(string)
	default:
		optContent = onecloud.GetConfig(opt).YAMLString()
	}
	if err := os.WriteFile(
		cfgFilePath,
		[]byte(optContent),
		0644); err != nil {
		return errors.Wrapf(err, "write config file %q", cfgFilePath)
	}
	return nil
}

func (m *prepareManager) PostInit(cpnt Component, oc *v1alpha1.OnecloudCluster, targetCfgDir string) error {
	log.Infof("Start post init component: %q", cpnt.GetType())
	pCtrl := m.getComponentPhaseControl(cpnt, oc)
	if pCtrl != nil {
		if err := pCtrl.SystemInit(oc); err != nil {
			return errors.Wrapf(err, "init system of component %q", cpnt.GetType())
		}
	}

	return nil
}

func GetRCAdminFilePath(cfgDir string) string {
	return filepath.Join(cfgDir, YUNION_ETC_CONFIG_DIR, "rcadmin")
}
