// Copyright 2019 Yunion
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package component

import (
	"context"
	"fmt"

	apps "k8s.io/api/apps/v1"
	jobbatchv1 "k8s.io/api/batch/v1"
	batchv1 "k8s.io/api/batch/v1beta1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	appv1 "k8s.io/client-go/listers/apps/v1"
	batchlisters "k8s.io/client-go/listers/batch/v1beta1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"

	"yunion.io/x/log"
	errorswrap "yunion.io/x/pkg/errors"
	"yunion.io/x/pkg/utils"

	"yunion.io/x/onecloud/pkg/mcclient"

	"yunion.io/x/onecloud-operator/pkg/apis/constants"
	"yunion.io/x/onecloud-operator/pkg/apis/onecloud/v1alpha1"
	"yunion.io/x/onecloud-operator/pkg/controller"
	"yunion.io/x/onecloud-operator/pkg/label"
	"yunion.io/x/onecloud-operator/pkg/manager"
	"yunion.io/x/onecloud-operator/pkg/service-init/component"
	"yunion.io/x/onecloud-operator/pkg/util/k8sutil"
	"yunion.io/x/onecloud-operator/pkg/util/onecloud"
)

type ComponentManager struct {
	kubeCli kubernetes.Interface

	deployControl controller.DeploymentControlInterface
	deployLister  appv1.DeploymentLister
	svcControl    controller.ServiceControlInterface
	svcLister     corelisters.ServiceLister
	pvcControl    controller.PVCControlInterface
	pvcLister     corelisters.PersistentVolumeClaimLister
	ingControl    controller.IngressControlInterface
	ingLister     cache.GenericLister
	dsControl     controller.DaemonSetControlInterface
	dsLister      appv1.DaemonSetLister
	cronControl   controller.CronJobControlInterface
	cronLister    batchlisters.CronJobLister
	nodeLister    corelisters.NodeLister

	configer               Configer
	onecloudControl        *controller.OnecloudControl
	onecloudClusterControl controller.ClusterControlInterface
	clusterVersion         k8sutil.ClusterVersion
}

// NewComponentManager return *BaseComponentManager
func NewComponentManager(
	kubeCli kubernetes.Interface,
	deployCtrol controller.DeploymentControlInterface,
	deployLister appv1.DeploymentLister,
	svcControl controller.ServiceControlInterface,
	svcLister corelisters.ServiceLister,
	pvcControl controller.PVCControlInterface,
	pvcLister corelisters.PersistentVolumeClaimLister,
	ingControl controller.IngressControlInterface,
	ingLister cache.GenericLister,
	dsControl controller.DaemonSetControlInterface,
	dsLister appv1.DaemonSetLister,
	cronControl controller.CronJobControlInterface,
	cronLister batchlisters.CronJobLister,
	nodeLister corelisters.NodeLister,
	configer Configer,
	onecloudControl *controller.OnecloudControl,
	onecloudClusterControl controller.ClusterControlInterface,
	clusterVersion k8sutil.ClusterVersion,
) *ComponentManager {
	return &ComponentManager{
		kubeCli:                kubeCli,
		deployControl:          deployCtrol,
		deployLister:           deployLister,
		svcControl:             svcControl,
		svcLister:              svcLister,
		pvcControl:             pvcControl,
		pvcLister:              pvcLister,
		ingControl:             ingControl,
		ingLister:              ingLister,
		dsControl:              dsControl,
		dsLister:               dsLister,
		cronControl:            cronControl,
		cronLister:             cronLister,
		nodeLister:             nodeLister,
		configer:               configer,
		onecloudControl:        onecloudControl,
		onecloudClusterControl: onecloudClusterControl,
		clusterVersion:         clusterVersion,
	}
}

func (m *ComponentManager) RunWithSession(oc *v1alpha1.OnecloudCluster, f func(s *mcclient.ClientSession) error) error {
	return m.onecloudControl.RunWithSession(oc, f)
}

func (m *ComponentManager) syncService(
	oc *v1alpha1.OnecloudCluster,
	factory cloudComponentFactory,
	zone string,
) error {
	ns := oc.GetNamespace()
	cfg, err := m.configer.GetClusterConfig(oc)
	if err != nil {
		return errorswrap.Wrap(err, "GetClusterConfig")
	}
	svcFactory := factory.getService
	newSvcs := svcFactory(oc, cfg, zone)
	if len(newSvcs) == 0 {
		return nil
	}
	inPV := isInProductVersion(factory, oc)
	for _, newSvc := range newSvcs {
		oldSvcTmp, err := m.svcLister.Services(ns).Get(newSvc.GetName())
		if err != nil {
			if errors.IsNotFound(err) {
				if !inPV {
					return nil
				}
				if err := SetServiceLastAppliedConfigAnnotation(newSvc); err != nil {
					return err
				}
				return m.svcControl.CreateService(oc, newSvc)
			} else {
				return err
			}
		}

		if !inPV {
			return m.svcControl.DeleteService(oc, oldSvcTmp)
		}

		oldSvc := oldSvcTmp.DeepCopy()

		equal, err := serviceEqual(newSvc, oldSvc)
		if err != nil {
			return err
		}
		if !equal {
			svc := *oldSvc
			svc.Spec = newSvc.Spec
			svc.Spec.ClusterIP = oldSvc.Spec.ClusterIP
			err = SetServiceLastAppliedConfigAnnotation(&svc)
			if err != nil {
				return err
			}
			_, err = m.svcControl.UpdateService(oc, &svc)
			return err
		}
	}
	return nil
}

func (m *ComponentManager) syncIngress(
	oc *v1alpha1.OnecloudCluster,
	ingFactory cloudComponentFactory,
	zone string,
) error {
	ns := oc.GetNamespace()
	newF := ingFactory.getIngress
	updateF := ingFactory.updateIngress
	newIng := newF(oc, zone)
	if newIng == nil {
		return nil
	}
	inPV := isInProductVersion(ingFactory, oc)
	oldIngTmp, err := m.ingLister.ByNamespace(ns).Get(newIng.GetName())
	if err != nil {
		if errors.IsNotFound(err) {
			if !inPV {
				return nil
			}
			if err := SetIngressLastAppliedConfigAnnotation(newIng); err != nil {
				return err
			}
			return m.ingControl.CreateIngress(oc, newIng)
		}
	}

	if !inPV {
		return m.ingControl.DeleteIngress(oc, oldIngTmp.(*unstructured.Unstructured))
	}

	// update old ingress
	oldIng := oldIngTmp.(*unstructured.Unstructured).DeepCopy()

	equal, err := ingressEqual(newIng, oldIng)
	if err != nil {
		return err
	}
	if !equal {
		ing := updateF(oc, oldIng)
		err = SetIngressLastAppliedConfigAnnotation(ing)
		if err != nil {
			return err
		}
		_, err = m.ingControl.UpdateIngress(oc, ing)
		return err
	}
	return nil
}

func (m *ComponentManager) syncConfigMap(
	oc *v1alpha1.OnecloudCluster,
	f cloudComponentFactory,
	zone string,
) error {
	inPV := isInProductVersion(f, oc)
	if !inPV {
		return nil
	}
	dbConfigFactory := f.getDBConfig
	svcAccountFactory := f.getCloudUser
	cfgMapFactory := f.getConfigMap
	clustercfg, err := m.configer.GetClusterConfig(oc)
	if err != nil {
		return errorswrap.Wrap(err, "get cluster config")
	}
	{
		dbConfig := dbConfigFactory(clustercfg)
		if dbConfig != nil {
			dbEngine := f.getDBEngine(oc)
			switch dbEngine {
			case v1alpha1.DBEngineDameng:
				if err := component.EnsureClusterDamengUser(oc, *dbConfig); err != nil {
					return errorswrap.Wrap(err, "ensure cluster dameng db user")
				}
			case v1alpha1.DBEngineMySQL:
				fallthrough
			default:
				if err := component.EnsureClusterMySQLUser(oc, *dbConfig); err != nil {
					return errorswrap.Wrap(err, "ensure cluster mysql db user")
				}
			}

		}
	}
	if len(oc.Spec.Clickhouse.Host) > 0 {
		clickhouseConfig := f.getClickhouseConfig(clustercfg)
		if clickhouseConfig != nil && IsEnterpriseEdition(oc) {
			if err := component.EnsureClusterClickhouseUser(oc, *clickhouseConfig); err != nil {
				return errorswrap.Wrap(err, "ensure clickhouse cluster db user")
			}
		}
	}
	{
		account := svcAccountFactory(clustercfg)
		if account != nil {
			if err := m.onecloudControl.RunWithSession(oc, func(s *mcclient.ClientSession) error {
				if err := component.EnsureServiceAccount(s, *account); err != nil {
					return errorswrap.Wrapf(err, "ensure service account %#v", *account)
				}
				return nil
			}); err != nil {
				if controller.StopServices {
					log.Warningf("RunWithSession when stop-services: %s", err)
					return nil
				}
				return errorswrap.Wrap(err, "RunWithSession")
			}
		}
	}
	cfgMap, forceUpdate, err := cfgMapFactory(oc, clustercfg, zone)
	if err != nil {
		return err
	}
	if cfgMap == nil {
		return nil
	}
	if err := SetConfigMapLastAppliedConfigAnnotation(cfgMap); err != nil {
		return err
	}
	if forceUpdate {
		log.Infof("forceUpdate configMap %s", cfgMap.GetName())
		return m.configer.CreateOrUpdateConfigMap(oc, cfgMap)
	}
	oldCfgMap, err := m.configer.Lister().ConfigMaps(oc.GetNamespace()).Get(cfgMap.GetName())
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	if oldCfgMap != nil {
		//if equal, err := configMapEqual(cfgMap, oldCfgMap); err != nil {
		//	return err
		//} else if equal {
		//	return nil
		//}
		// if cfgmap exist do not update
		return nil
	}
	return m.configer.CreateOrUpdateConfigMap(oc, cfgMap)
}

func (m *ComponentManager) syncDaemonSet(
	oc *v1alpha1.OnecloudCluster,
	f cloudComponentFactory,
	zone string,
) error {
	ns := oc.GetNamespace()
	cfg, err := m.configer.GetClusterConfig(oc)
	if err != nil {
		return err
	}
	dsFactory := f.getDaemonSet
	newDs, err := dsFactory(oc, cfg, zone)
	if err != nil {
		return err
	}
	if newDs == nil {
		return nil
	}

	inPV := isInProductVersion(f, oc)
	shouldStop := controller.StopServices && !utils.IsInStringArray(f.GetComponentType().String(), []string{
		v1alpha1.HostComponentType.String(),
	})

	oldDsTmp, err := m.dsLister.DaemonSets(ns).Get(newDs.GetName())
	if err != nil {
		if errors.IsNotFound(err) {
			if !inPV || shouldStop {
				return nil
			}
			return m.dsControl.CreateDaemonSet(oc, newDs)
		} else {
			return err
		}
	}

	if !inPV || shouldStop {
		return m.dsControl.DeleteDaemonSet(oc, oldDsTmp)
	}

	oldDs := oldDsTmp.DeepCopy()
	if err = m.updateDaemonSet(oc, newDs, oldDs); err != nil {
		return err
	}
	return nil
}

func (m *ComponentManager) updateDaemonSet(oc *v1alpha1.OnecloudCluster, newDs, oldDs *apps.DaemonSet) error {
	if !daemonSetEqual(newDs, oldDs) {
		ds := *oldDs
		ds.Spec.Template = newDs.Spec.Template
		ds.Spec.UpdateStrategy = newDs.Spec.UpdateStrategy
		err := SetDaemonSetLastAppliedConfigAnnotation(&ds)
		if err != nil {
			return err
		}
		_, err = m.dsControl.UpdateDaemonSet(oc, &ds)
		return err
	}
	return nil
}

func (m *ComponentManager) syncCronJob(
	oc *v1alpha1.OnecloudCluster,
	f cloudComponentFactory,
	zone string,
) error {
	cronFactory := f.getCronJob
	cfg, err := m.configer.GetClusterConfig(oc)
	if err != nil {
		return err
	}
	newCronJob, err := cronFactory(oc, cfg, zone)
	if err != nil {
		return err
	}
	if newCronJob == nil {
		return nil
	}

	inPV := isInProductVersion(f, oc)

	oldCronJob, err := m.cronLister.CronJobs(oc.GetNamespace()).Get(newCronJob.GetName())
	if err != nil {
		if errors.IsNotFound(err) {
			if !inPV {
				return nil
			}
			return m.cronControl.CreateCronJob(oc, newCronJob)
		}
		return errorswrap.Wrap(err, "sync cronjob on list")
	}

	if !inPV {
		return m.cronControl.DeleteCronJob(oc, oldCronJob.GetName())
	}

	_oldCronJob := oldCronJob.DeepCopy()
	if err = m.updateCronJob(oc, newCronJob, _oldCronJob); err != nil {
		return err
	}
	return nil
}

func (m *ComponentManager) updateCronJob(oc *v1alpha1.OnecloudCluster, newCronJob, oldCronJob *batchv1.CronJob) error {
	if !cronJobEqual(newCronJob, oldCronJob) {
		klog.Infof("start update cron job %s", newCronJob.GetName())
		cronJob := *oldCronJob
		cronJob.Spec = newCronJob.Spec
		err := SetCronJobLastAppliedConfigAnnotation(&cronJob)
		if err != nil {
			return err
		}
		_, err = m.cronControl.UpdateCronJob(oc, &cronJob)
		if err != nil {
			return errorswrap.Wrap(err, "update cron job")
		}
		return nil
	}
	return nil
}

func (m *ComponentManager) syncDeployment(
	oc *v1alpha1.OnecloudCluster,
	f cloudComponentFactory,
	postSyncFunc func(*v1alpha1.OnecloudCluster, *apps.Deployment, string) error,
	zone string,
) error {
	ns := oc.GetNamespace()
	cfg, err := m.configer.GetClusterConfig(oc)
	if err != nil {
		return errorswrap.Wrapf(err, "GetClusterConfig for component: %s", f.GetComponentType())
	}
	deploymentFactory := f.getDeployment
	newDeploy, err := deploymentFactory(oc, cfg, zone)
	if err != nil {
		return errorswrap.Wrapf(err, "deploymentFactory for component: %s", f.GetComponentType())
	}
	if newDeploy == nil {
		return nil
	}

	shouldStop := controller.StopServices && !utils.IsInStringArray(f.GetComponentType().String(), []string{
		v1alpha1.OvnNorthComponentType.String(),
		v1alpha1.InfluxdbComponentType.String(),
	})

	inPV := isInProductVersion(f, oc)

	postFunc := func(deploy *apps.Deployment) error {
		if postSyncFunc != nil {
			deploy, err := m.kubeCli.AppsV1().Deployments(deploy.GetNamespace()).Get(context.Background(), deploy.GetName(), metav1.GetOptions{})
			if err != nil {
				return errorswrap.Wrapf(err, "get deployment %s", deploy.GetName())
			}
			return postSyncFunc(oc, deploy, zone)
		}
		return nil
	}

	oldDeployTmp, err := m.deployLister.Deployments(ns).Get(newDeploy.GetName())
	if err != nil {
		log.Errorf("get old deployment error for component %s: %s", f.GetComponentType(), err)
		if errors.IsNotFound(err) {
			if !inPV || shouldStop {
				return nil
			}
			if err := SetDeploymentLastAppliedConfigAnnotation(newDeploy); err != nil {
				return err
			}
			if err = m.deployControl.CreateDeployment(oc, newDeploy); err != nil {
				return err
			}
			if err := postFunc(newDeploy); err != nil {
				return errorswrap.Wrapf(err, "post create deployment %s", newDeploy.GetName())
			}
			return controller.RequeueErrorf("OnecloudCluster: [%s/%s], waiting for %s deployment running", ns, oc.GetName(), newDeploy.GetName())

		} else {
			return err
		}
	}

	oldDeploy := oldDeployTmp.DeepCopy()
	if !inPV || shouldStop {
		return m.deployControl.DeleteDeployment(oc, oldDeploy.Name)
	}

	if err = m.updateDeployment(oc, newDeploy, oldDeploy); err != nil {
		return err
	}
	return postFunc(oldDeploy)
}

func (m *ComponentManager) updateDeployment(oc *v1alpha1.OnecloudCluster, newDeploy, oldDeploy *apps.Deployment) error {
	if !deploymentEqual(*newDeploy, *oldDeploy) {
		deploy := *oldDeploy
		deploy.Spec.Template = newDeploy.Spec.Template
		*deploy.Spec.Replicas = *newDeploy.Spec.Replicas
		deploy.Spec.Strategy = newDeploy.Spec.Strategy
		err := SetDeploymentLastAppliedConfigAnnotation(&deploy)
		if err != nil {
			return err
		}
		_, err = m.deployControl.UpdateDeployment(oc, &deploy)
		return err
	}
	return nil
}

func (m *ComponentManager) newDefaultDeployment(
	componentType v1alpha1.ComponentType,
	zoneComponentType v1alpha1.ComponentType,
	oc *v1alpha1.OnecloudCluster,
	volHelper *VolumeHelper,
	spec *v1alpha1.DeploymentSpec,
	initContainersFactory func([]corev1.VolumeMount) []corev1.Container, containersFactory func([]corev1.VolumeMount) []corev1.Container,
) (*apps.Deployment, error) {
	return m.newDefaultDeploymentWithCloudAffinity(componentType, zoneComponentType, oc, volHelper, spec, initContainersFactory, containersFactory)
}

func (m *ComponentManager) removeDeploymentAffinity(deploy *apps.Deployment) *apps.Deployment {
	deploy.Spec.Template.Spec.Affinity = nil
	return deploy
}

func (m *ComponentManager) SetComponentAffinity(spec *v1alpha1.DeploymentSpec) {
	if spec.Affinity == nil {
		spec.Affinity = &corev1.Affinity{}
	}
	if spec.Affinity.NodeAffinity == nil {
		spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	if !controller.DisableNodeSelectorController {
		spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{
							Key:      constants.OnecloudControllerLabelKey,
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"enable"},
						},
					},
				},
			},
		}
	}
}

func (m *ComponentManager) newDefaultDeploymentWithCloudAffinity(
	componentType v1alpha1.ComponentType,
	zoneComponentType v1alpha1.ComponentType,
	oc *v1alpha1.OnecloudCluster,
	volHelper *VolumeHelper,
	spec *v1alpha1.DeploymentSpec,
	initContainersFactory func([]corev1.VolumeMount) []corev1.Container,
	containersFactory func([]corev1.VolumeMount) []corev1.Container,
) (*apps.Deployment, error) {
	if spec.Affinity == nil {
		spec.Affinity = &corev1.Affinity{}
	}
	if spec.Affinity.NodeAffinity == nil && !controller.DisableNodeSelectorController {
		spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
		spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{
							Key:      constants.OnecloudControllerLabelKey,
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"enable"},
						},
					},
				},
			},
		}
	}
	return m.newDeployment(componentType, zoneComponentType, oc, volHelper, spec, initContainersFactory, containersFactory, false, corev1.DNSClusterFirst)
}

func (m *ComponentManager) newDefaultDeploymentNoInit(
	componentType v1alpha1.ComponentType, zoneComponentType v1alpha1.ComponentType,
	oc *v1alpha1.OnecloudCluster, volHelper *VolumeHelper, spec *v1alpha1.DeploymentSpec,
	containersFactory func([]corev1.VolumeMount) []corev1.Container,
) (*apps.Deployment, error) {
	return m.newDefaultDeploymentWithCloudAffinity(componentType, zoneComponentType, oc, volHelper, spec, nil, containersFactory)
}

func (m *ComponentManager) newDefaultDeploymentNoInitWithoutCloudAffinity(
	componentType v1alpha1.ComponentType,
	zoneComponentType v1alpha1.ComponentType,
	oc *v1alpha1.OnecloudCluster,
	volHelper *VolumeHelper,
	spec *v1alpha1.DeploymentSpec,
	hostNetwork bool,
	containersFactory func([]corev1.VolumeMount) []corev1.Container,
) (*apps.Deployment, error) {
	dnsPolicy := corev1.DNSClusterFirst
	if hostNetwork {
		dnsPolicy = corev1.DNSClusterFirstWithHostNet
	}
	return m.newDeployment(componentType, zoneComponentType, oc, volHelper, spec, nil, containersFactory, hostNetwork, dnsPolicy)
}

func (m *ComponentManager) getObjectMeta(oc *v1alpha1.OnecloudCluster, name string, labels map[string]string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:            name,
		Namespace:       oc.GetNamespace(),
		Labels:          labels,
		OwnerReferences: []metav1.OwnerReference{controller.GetOwnerRef(oc)},
	}
}

func (m *ComponentManager) getComponentLabel(oc *v1alpha1.OnecloudCluster, componentType v1alpha1.ComponentType) label.Label {
	instanceName := oc.GetLabels()[label.InstanceLabelKey]
	return label.New().Instance(instanceName).Component(componentType.String())
}

func (m *ComponentManager) newConfigMap(componentType v1alpha1.ComponentType, zone string, oc *v1alpha1.OnecloudCluster, config string) *corev1.ConfigMap {
	name := controller.ComponentConfigMapName(oc, m.getZoneComponent(componentType, zone))
	labels := m.getComponentLabel(oc, componentType)
	labels = labels.Zone(zone)
	return &corev1.ConfigMap{
		ObjectMeta: m.getObjectMeta(oc, name, labels.Labels()),
		Data: map[string]string{
			"config": config,
		},
	}
}

func (m *ComponentManager) newServiceConfigMap(cType v1alpha1.ComponentType, zone string, oc *v1alpha1.OnecloudCluster, opt interface{}) *corev1.ConfigMap {
	configYaml := onecloud.GetConfig(opt).YAMLString()
	return m.newConfigMap(cType, zone, oc, configYaml)
}

func (m *ComponentManager) newService(
	componentType v1alpha1.ComponentType,
	oc *v1alpha1.OnecloudCluster,
	serviceType corev1.ServiceType,
	ports []corev1.ServicePort,
) *corev1.Service {
	ocName := oc.GetName()
	svcName := controller.NewClusterComponentName(ocName, componentType)
	appLabel := m.getComponentLabel(oc, componentType)

	svc := &corev1.Service{
		ObjectMeta: m.getObjectMeta(oc, svcName, appLabel),
		Spec: corev1.ServiceSpec{
			Type:     serviceType,
			Selector: appLabel,
			Ports:    ports,
		},
	}
	return svc
}

func (m *ComponentManager) newServiceWithClusterIp(
	componentType v1alpha1.ComponentType,
	oc *v1alpha1.OnecloudCluster,
	serviceType corev1.ServiceType,
	ports []corev1.ServicePort,
	clusterIp string,
) *corev1.Service {
	ocName := oc.GetName()
	svcName := controller.NewClusterComponentName(ocName, componentType)
	appLabel := m.getComponentLabel(oc, componentType)

	svc := &corev1.Service{
		ObjectMeta: m.getObjectMeta(oc, svcName, appLabel),
		Spec: corev1.ServiceSpec{
			Type:      serviceType,
			Selector:  appLabel,
			Ports:     ports,
			ClusterIP: clusterIp,
		},
	}
	return svc
}

func (m *ComponentManager) newNodePortService(cType v1alpha1.ComponentType, oc *v1alpha1.OnecloudCluster, ports []corev1.ServicePort) *corev1.Service {
	return m.newService(cType, oc, corev1.ServiceTypeNodePort, ports)
}

func (m *ComponentManager) newSingleNodePortService(cType v1alpha1.ComponentType, oc *v1alpha1.OnecloudCluster, nodePort, targetPort int32) *corev1.Service {
	ports := []corev1.ServicePort{
		NewServiceNodePort("api", nodePort, targetPort),
	}
	return m.newNodePortService(cType, oc, ports)
}

func (m *ComponentManager) newEtcdClientService(
	cType v1alpha1.ComponentType, oc *v1alpha1.OnecloudCluster,
) *corev1.Service {
	ports := []corev1.ServicePort{{
		Name:       "client",
		Port:       constants.EtcdClientPort,
		TargetPort: intstr.FromInt(constants.EtcdClientPort),
		Protocol:   corev1.ProtocolTCP,
	}}
	return m.newService(cType, oc, corev1.ServiceTypeClusterIP, ports)
}

func (m *ComponentManager) newEtcdService(
	cType v1alpha1.ComponentType, oc *v1alpha1.OnecloudCluster,
) *corev1.Service {
	ports := []corev1.ServicePort{{
		Name:       "client",
		Port:       constants.EtcdClientPort,
		TargetPort: intstr.FromInt(constants.EtcdClientPort),
		Protocol:   corev1.ProtocolTCP,
	}, {
		Name:       "peer",
		Port:       constants.EtcdPeerPort,
		TargetPort: intstr.FromInt(constants.EtcdPeerPort),
		Protocol:   corev1.ProtocolTCP,
	}}
	return m.newServiceWithClusterIp(
		cType, oc, corev1.ServiceTypeClusterIP, ports, corev1.ClusterIPNone)
}

func (m *ComponentManager) newDeployment(
	componentType v1alpha1.ComponentType, zoneComponentType v1alpha1.ComponentType,
	oc *v1alpha1.OnecloudCluster, volHelper *VolumeHelper, spec *v1alpha1.DeploymentSpec,
	initContainersFactory func([]corev1.VolumeMount) []corev1.Container, containersFactory func([]corev1.VolumeMount) []corev1.Container,
	hostNetwork bool, dnsPolicy corev1.DNSPolicy,
) (*apps.Deployment, error) {
	if len(zoneComponentType) == 0 {
		zoneComponentType = componentType
	}
	ns := oc.GetNamespace()
	ocName := oc.GetName()

	appLabel := m.getComponentLabel(oc, componentType)

	vols := volHelper.GetVolumes()
	volMounts := volHelper.GetVolumeMounts()

	podAnnotations := spec.Annotations

	deployName := controller.NewClusterComponentName(ocName, zoneComponentType)

	appDeploy := &apps.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:            deployName,
			Namespace:       ns,
			Labels:          appLabel.Labels(),
			OwnerReferences: []metav1.OwnerReference{controller.GetOwnerRef(oc)},
		},
		Spec: apps.DeploymentSpec{
			Replicas: &spec.Replicas,
			Strategy: apps.DeploymentStrategy{Type: apps.RollingUpdateDeploymentStrategyType},
			Selector: appLabel.LabelSelector(),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      appLabel.Labels(),
					Annotations: podAnnotations,
				},
				Spec: corev1.PodSpec{
					Affinity:         spec.Affinity,
					NodeSelector:     spec.NodeSelector,
					Containers:       containersFactory(volMounts),
					RestartPolicy:    corev1.RestartPolicyAlways,
					Tolerations:      spec.Tolerations,
					Volumes:          vols,
					HostNetwork:      hostNetwork,
					DNSPolicy:        dnsPolicy,
					DNSConfig:        spec.DNSConfig,
					ImagePullSecrets: spec.ImagePullSecrets,
				},
			},
		},
	}
	templateSpec := &appDeploy.Spec.Template.Spec
	if initContainersFactory != nil {
		templateSpec.InitContainers = initContainersFactory(volMounts)
	}
	if containersFactory != nil {
		templateSpec.Containers = containersFactory(volMounts)
	}

	if err := m.setContainerResources(oc.Spec.DisableResourceManagement, &spec.ContainerSpec, templateSpec); err != nil {
		log.Errorf("set container resources %v", err)
	}

	if hostNetwork {
		appDeploy.Spec.Strategy = apps.DeploymentStrategy{Type: apps.RecreateDeploymentStrategyType}
	}

	return appDeploy, nil
}

func (m *ComponentManager) setContainerResources(disable bool, spec *v1alpha1.ContainerSpec, templateSpec *corev1.PodSpec) error {
	if disable {
		if spec.Limits != nil {
			if spec.Limits.CPU != nil {
				spec.Limits.CPU = nil
			}
			if spec.Limits.Memory != nil {
				spec.Limits.Memory = nil
			}
		}
		if spec.Requests != nil {
			if spec.Requests.CPU != nil {
				spec.Requests.CPU = nil
			}
			if spec.Requests.Memory != nil {
				spec.Requests.Memory = nil
			}
		}
		return nil
	}
	// process resources limits
	if spec.Limits == nil {
		spec.Limits = new(v1alpha1.ResourceRequirement)
	}
	// set default limits by node capacity
	cpu, mem, err := m.getDefaultResourceLimits()
	if err != nil {
		return errorswrap.Wrap(err, "get default cpu and memory limits")
	}
	if spec.Limits.CPU == nil {
		spec.Limits.CPU = cpu
	}
	if spec.Limits.Memory == nil {
		spec.Limits.Memory = mem
	}

	// process resources requests
	if spec.Requests == nil {
		spec.Requests = new(v1alpha1.ResourceRequirement)
	}
	// hack: set default request cpu and memory requests
	reqCPU, _ := resource.ParseQuantity("0.01")
	reqMem, _ := resource.ParseQuantity("10Mi")
	if spec.Requests.CPU == nil {
		spec.Requests.CPU = &reqCPU
	}
	if spec.Requests.Memory == nil {
		spec.Requests.Memory = &reqMem
	}

	// set deployment resources limits and request
	m.setPodSpecResources(templateSpec, corev1.ResourceCPU, spec.Limits.CPU, spec.Requests.CPU)
	m.setPodSpecResources(templateSpec, corev1.ResourceMemory, spec.Limits.Memory, spec.Requests.Memory)

	return nil
}

func (m *ComponentManager) setPodSpecResources(
	spec *corev1.PodSpec, key corev1.ResourceName,
	iLimit, iReq *resource.Quantity) {
	if iLimit == nil || iReq == nil {
		return
	}
	for idx, ctr := range spec.Containers {
		if ctr.Resources.Limits == nil {
			ctr.Resources.Limits = make(map[corev1.ResourceName]resource.Quantity)
		}
		if ctr.Resources.Requests == nil {
			ctr.Resources.Requests = make(map[corev1.ResourceName]resource.Quantity)
		}
		ctr.Resources.Limits[key] = *iLimit
		ctr.Resources.Requests[key] = *iReq
		spec.Containers[idx] = ctr
	}
}

func (m *ComponentManager) getDefaultResourceLimits() (*resource.Quantity, *resource.Quantity, error) {
	nodes, err := k8sutil.GetReadyMasterNodes(m.nodeLister)
	if err != nil {
		return nil, nil, errorswrap.Wrap(err, "get ready master nodes")
	}
	if len(nodes) == 0 {
		return nil, nil, errorswrap.Wrap(err, "get 0 ready master node")
	}
	node := nodes[0]
	cpu, mem, err := m.getNodeCapacity(node)
	if err != nil {
		return nil, nil, errorswrap.Wrapf(err, "get node %s capacity", node.GetName())
	}
	cpuCount, _ := cpu.AsInt64()
	memBytes, _ := mem.AsInt64()
	defCPUStr := fmt.Sprintf("%f", float64(cpuCount)/3.0)
	defMemMB := memBytes / 1000 / 1000 / 4
	if defMemMB < 1024 {
		defMemMB = 1024
	}
	defMemMBStr := fmt.Sprintf("%dMi", defMemMB)

	defCPU, err := resource.ParseQuantity(defCPUStr)
	if err != nil {
		return nil, nil, errorswrap.Wrapf(err, "parse cpu %s", defCPUStr)
	}
	defMem, err := resource.ParseQuantity(defMemMBStr)
	if err != nil {
		return nil, nil, errorswrap.Wrapf(err, "parse mem %s", defMemMBStr)
	}

	return &defCPU, &defMem, nil
}

func (m *ComponentManager) getNodeCapacity(node *corev1.Node) (*resource.Quantity, *resource.Quantity, error) {
	capacity := node.Status.Capacity
	// nodeCpuInt, ok := capacity.Cpu().AsInt64()
	_, ok := capacity.Cpu().AsInt64()
	if !ok {
		return nil, nil, errorswrap.Errorf("node %s cpu as int64", capacity.Cpu())
	}
	// nodeMemInt, ok := capacity.Memory().AsInt64()
	_, ok = capacity.Memory().AsInt64()
	if !ok {
		return nil, nil, errorswrap.Errorf("node %s memory as int64", capacity.Memory())
	}
	// log.Infof("get node %s cpu int: %d, memory int: %d", node.GetName(), nodeCpuInt, nodeMemInt)
	return capacity.Cpu(), capacity.Memory(), nil
}

func generateLivenessProbe(path string, port int32) *v1.Probe {
	return &v1.Probe{
		InitialDelaySeconds: 60,
		PeriodSeconds:       5,
		FailureThreshold:    3,
		SuccessThreshold:    1,
		TimeoutSeconds:      5,
		Handler: v1.Handler{
			HTTPGet: &v1.HTTPGetAction{
				Path: path,
				Port: intstr.IntOrString{
					Type:   intstr.Int,
					IntVal: port,
				},
				Scheme: corev1.URISchemeHTTPS,
			},
		},
	}
}

func generateReadinessProbe(path string, port int32) *v1.Probe {
	return &v1.Probe{
		InitialDelaySeconds: 15,
		PeriodSeconds:       5,
		FailureThreshold:    2,
		SuccessThreshold:    3,
		TimeoutSeconds:      3,
		Handler: v1.Handler{
			HTTPGet: &v1.HTTPGetAction{
				Path: path,
				Port: intstr.IntOrString{
					Type:   intstr.Int,
					IntVal: port,
				},
				Scheme: corev1.URISchemeHTTPS,
			},
		},
	}
}

func (m *ComponentManager) newCloudServiceDeployment(
	cType v1alpha1.ComponentType, zoneComponentType v1alpha1.ComponentType,
	oc *v1alpha1.OnecloudCluster, deployCfg *v1alpha1.DeploymentSpec,
	initContainersF func([]corev1.VolumeMount) []corev1.Container,
	ports []corev1.ContainerPort, mountEtcdTLS bool, keepAffinity bool,
	readinessProbePath string,
) (*apps.Deployment, error) {
	var readyProbe *v1.Probe
	// var liveProbe *v1.Probe
	if len(readinessProbePath) > 0 {
		readyProbe = generateReadinessProbe(readinessProbePath, ports[0].ContainerPort)
		// liveProbe = generateLivenessProbe(readinessProbePath, ports[0].ContainerPort)
	}
	configMap := controller.ComponentConfigMapName(oc, zoneComponentType)
	containersF := func(volMounts []corev1.VolumeMount) []corev1.Container {
		return []corev1.Container{
			{
				Name:  cType.String(),
				Image: deployCfg.Image,
				Command: []string{
					fmt.Sprintf("/opt/yunion/bin/%s", cType.String()),
					"--config",
					fmt.Sprintf("/etc/yunion/%s.conf", cType.String()),
				},
				ImagePullPolicy: deployCfg.ImagePullPolicy,
				Ports:           ports,
				VolumeMounts:    volMounts,
				ReadinessProbe:  readyProbe,
				// LivenessProbe:   liveProbe,
			},
		}
	}

	var h *VolumeHelper
	if mountEtcdTLS {
		h = NewVolumeHelperWithEtcdTLS(oc, configMap, cType)
	} else {
		h = NewVolumeHelper(oc, configMap, cType)
	}

	deploy, err := m.newDefaultDeployment(cType, zoneComponentType, oc, h, deployCfg, initContainersF, containersF)
	if err != nil {
		return nil, err
	}
	if keepAffinity {
		return deploy, nil
	}
	return m.removeDeploymentAffinity(deploy), nil
}

func (m *ComponentManager) newCloudServiceDeploymentWithInit(
	cType, zoneComponentType v1alpha1.ComponentType,
	oc *v1alpha1.OnecloudCluster,
	deployCfg *v1alpha1.DeploymentSpec,
	ports []corev1.ContainerPort,
	mountEtcdTLS bool,
	readinessProbePath string,
) (*apps.Deployment, error) {
	initContainersF := func(volMounts []corev1.VolumeMount) []corev1.Container {
		return []corev1.Container{
			{
				Name:            "init",
				Image:           deployCfg.Image,
				ImagePullPolicy: deployCfg.ImagePullPolicy,
				Command: []string{
					fmt.Sprintf("/opt/yunion/bin/%s", cType.String()),
					"--config",
					fmt.Sprintf("/etc/yunion/%s.conf", cType.String()),
					"--auto-sync-table",
					"--exit-after-db-init",
				},
				VolumeMounts: volMounts,
			},
		}
	}
	if len(zoneComponentType) == 0 {
		zoneComponentType = cType
	}
	return m.newCloudServiceDeployment(cType, zoneComponentType, oc, deployCfg, initContainersF, ports, mountEtcdTLS, true, readinessProbePath)
}

func (m *ComponentManager) newCloudServiceDeploymentNoInit(
	cType, zoneComponentType v1alpha1.ComponentType,
	oc *v1alpha1.OnecloudCluster,
	deployCfg *v1alpha1.DeploymentSpec,
	ports []corev1.ContainerPort,
	mountEtcdTLS bool,
	readinessProbePath string,
) (*apps.Deployment, error) {
	if len(zoneComponentType) == 0 {
		zoneComponentType = cType
	}
	return m.newCloudServiceDeployment(cType, zoneComponentType, oc, deployCfg, nil, ports, mountEtcdTLS, true, readinessProbePath)
}

func (m *ComponentManager) newCloudServiceSinglePortDeployment(
	cType, zoneComponentType v1alpha1.ComponentType,
	oc *v1alpha1.OnecloudCluster,
	deployCfg *v1alpha1.DeploymentSpec,
	port int32, doInit bool, mountEtcdTLS bool,
) (*apps.Deployment, error) {
	return m.newCloudServiceSinglePortDeploymentWithReadinessProbePath(cType, zoneComponentType, oc, deployCfg, port, doInit, mountEtcdTLS, "")
}

func (m *ComponentManager) newCloudServiceSinglePortDeploymentWithReadinessProbePath(
	cType, zoneComponentType v1alpha1.ComponentType,
	oc *v1alpha1.OnecloudCluster,
	deployCfg *v1alpha1.DeploymentSpec,
	port int32, doInit bool, mountEtcdTLS bool,
	readinessProbePath string,
) (*apps.Deployment, error) {
	ports := []corev1.ContainerPort{
		{
			Name:          "api",
			ContainerPort: port,
			Protocol:      corev1.ProtocolTCP,
		},
	}
	f := m.newCloudServiceDeploymentNoInit
	if doInit {
		f = m.newCloudServiceDeploymentWithInit
	}
	return f(cType, zoneComponentType, oc, deployCfg, ports, mountEtcdTLS, readinessProbePath)
}

func (m *ComponentManager) deploymentIsUpgrading(deploy *apps.Deployment, oc *v1alpha1.OnecloudCluster) (bool, error) {
	if deploymentIsUpgrading(deploy) {
		return true, nil
	}
	return false, nil
}

func (m *ComponentManager) newDaemonSet(
	componentType v1alpha1.ComponentType,
	oc *v1alpha1.OnecloudCluster,
	cfg *v1alpha1.OnecloudClusterConfig,
	volHelper *VolumeHelper,
	spec v1alpha1.DaemonSetSpec, updateStrategy apps.DaemonSetUpdateStrategyType,
	initContainers []corev1.Container,
	containersFactory func([]corev1.VolumeMount) []corev1.Container,
) (*apps.DaemonSet, error) {
	ns := oc.GetNamespace()
	ocName := oc.GetName()
	appLabel := m.getComponentLabel(oc, componentType)
	vols := volHelper.GetVolumes()
	volMounts := volHelper.GetVolumeMounts()
	podAnnotations := spec.Annotations
	if len(updateStrategy) == 0 {
		updateStrategy = apps.RollingUpdateDaemonSetStrategyType
	}

	dsName := controller.NewClusterComponentName(ocName, componentType)
	appDaemonSet := &apps.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            dsName,
			Namespace:       ns,
			Labels:          appLabel.Labels(),
			OwnerReferences: []metav1.OwnerReference{controller.GetOwnerRef(oc)},
		},
		Spec: apps.DaemonSetSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      appLabel.Labels(),
					Annotations: podAnnotations,
				},
				Spec: corev1.PodSpec{
					Affinity:         spec.Affinity,
					NodeSelector:     spec.NodeSelector,
					Containers:       containersFactory(volMounts),
					InitContainers:   initContainers,
					RestartPolicy:    corev1.RestartPolicyAlways,
					Tolerations:      spec.Tolerations,
					Volumes:          vols,
					HostNetwork:      true,
					HostPID:          true,
					HostIPC:          true,
					DNSPolicy:        corev1.DNSClusterFirstWithHostNet,
					ImagePullSecrets: spec.ImagePullSecrets,
				},
			},
			Selector: appLabel.LabelSelector(),
			UpdateStrategy: apps.DaemonSetUpdateStrategy{
				Type: updateStrategy,
			},
		},
	}
	return appDaemonSet, nil
}

func (m *ComponentManager) newCronJob(
	componentType v1alpha1.ComponentType,
	oc *v1alpha1.OnecloudCluster,
	volHelper *VolumeHelper,
	spec *v1alpha1.CronJobSpec,
	initContainersFactory func([]corev1.VolumeMount) []corev1.Container,
	containersFactory func([]corev1.VolumeMount) []corev1.Container,
	hostNetwork bool, dnsPolicy corev1.DNSPolicy,
	concurrencyPolicy batchv1.ConcurrencyPolicy,
	startingDeadlineSeconds *int64, suspend *bool,
	successfulJobsHistoryLimit, failedJobsHistoryLimit *int32,
) (*batchv1.CronJob, error) {
	ns := oc.GetNamespace()
	ocName := oc.GetName()
	vols := volHelper.GetVolumes()
	volMounts := volHelper.GetVolumeMounts()
	appLabel := m.getComponentLabel(oc, componentType)
	podAnnotations := spec.Annotations
	cronJobName := controller.NewClusterComponentName(ocName, componentType)
	var jobSpecBackoffLimit int32 = 1

	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:            cronJobName,
			Namespace:       ns,
			Labels:          appLabel.Labels(),
			OwnerReferences: []metav1.OwnerReference{controller.GetOwnerRef(oc)},
		},
		Spec: batchv1.CronJobSpec{
			Schedule:                   spec.Schedule,
			StartingDeadlineSeconds:    startingDeadlineSeconds,
			ConcurrencyPolicy:          concurrencyPolicy,
			Suspend:                    suspend,
			SuccessfulJobsHistoryLimit: successfulJobsHistoryLimit,
			FailedJobsHistoryLimit:     failedJobsHistoryLimit,
			JobTemplate: batchv1.JobTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      appLabel.Labels(),
					Annotations: podAnnotations,
				},
				Spec: jobbatchv1.JobSpec{
					// Selector: appLabel.LabelSelector(),
					BackoffLimit: &jobSpecBackoffLimit,
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels:      appLabel.Labels(),
							Annotations: podAnnotations,
						},
						Spec: corev1.PodSpec{
							Affinity:         spec.Affinity,
							NodeSelector:     spec.NodeSelector,
							Containers:       containersFactory(volMounts),
							RestartPolicy:    corev1.RestartPolicyNever, // never restart
							Tolerations:      spec.Tolerations,
							Volumes:          vols,
							HostNetwork:      hostNetwork,
							DNSPolicy:        dnsPolicy,
							ImagePullSecrets: spec.ImagePullSecrets,
						},
					},
				},
			},
		},
	}
	templateSpec := &cronJob.Spec.JobTemplate.Spec.Template.Spec
	if initContainersFactory != nil {
		templateSpec.InitContainers = initContainersFactory(volMounts)
	}

	if err := m.setContainerResources(oc.Spec.DisableResourceManagement, &spec.ContainerSpec, templateSpec); err != nil {
		log.Errorf("set container resources %v", err)
	}

	return cronJob, nil
}

func (m *ComponentManager) newDefaultCronJob(
	componentType v1alpha1.ComponentType,
	oc *v1alpha1.OnecloudCluster,
	volHelper *VolumeHelper,
	spec *v1alpha1.CronJobSpec,
	initContainersFactory func([]corev1.VolumeMount) []corev1.Container,
	containersFactory func([]corev1.VolumeMount) []corev1.Container,
) (*batchv1.CronJob, error) {
	return m.newCronJob(componentType, oc, volHelper, spec, initContainersFactory,
		containersFactory, false, corev1.DNSClusterFirst, batchv1.ReplaceConcurrent, &(v1alpha1.StartingDeadlineSeconds), nil, nil, nil)
}

func (m *ComponentManager) newPvcName(ocName, storageClass string, cType v1alpha1.ComponentType) string {
	prefix := controller.NewClusterComponentName(ocName, cType)
	if storageClass != v1alpha1.DefaultStorageClass {
		return fmt.Sprintf("%s-%s", prefix, storageClass)
	} else {
		return prefix
	}
}

func (m *ComponentManager) newPVC(cType v1alpha1.ComponentType, oc *v1alpha1.OnecloudCluster, spec v1alpha1.StatefulDeploymentSpec) (*corev1.PersistentVolumeClaim, error) {
	ocName := oc.GetName()
	storageClass := spec.StorageClassName
	pvcName := m.newPvcName(ocName, storageClass, cType)

	size := spec.Requests.Storage
	sizeQ, err := resource.ParseQuantity(size)
	if err != nil {
		return nil, err
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: oc.GetNamespace(),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: sizeQ,
				},
			},
		},
	}
	if storageClass != "" {
		pvc.Spec.StorageClassName = &storageClass
	}
	return pvc, nil
}

func (m *ComponentManager) syncPVC(oc *v1alpha1.OnecloudCluster, f cloudComponentFactory, zone string) error {
	inPV := isInProductVersion(f, oc)
	if !inPV {
		return nil
	}
	pvcFactory := f.getPVC
	ns := oc.GetNamespace()
	newPvc, err := pvcFactory(oc, zone)
	if err != nil {
		return err
	}
	if newPvc == nil {
		return nil
	}
	_, err = m.pvcLister.PersistentVolumeClaims(ns).Get(newPvc.GetName())
	if errors.IsNotFound(err) {
		return m.pvcControl.CreatePVC(oc, newPvc)
	}
	if err != nil {
		return err
	}
	// Not update pvc
	return nil
}

func (m *ComponentManager) syncPhase(
	oc *v1alpha1.OnecloudCluster,
	f cloudComponentFactory,
	zone string,
) error {
	if controller.StopServices {
		return nil
	}
	phaseFactory := f.getPhaseControl
	phase := phaseFactory(m.onecloudControl.Components(oc), zone)
	if phase == nil {
		return nil
	}
	inPV := isInProductVersion(f, oc)
	if !inPV {
		return nil
	}
	if err := phase.Setup(); err != nil {
		return err
	}
	if err := phase.SystemInit(oc); err != nil {
		return err
	}
	return nil
}

func (m *ComponentManager) multiZoneSync(oc *v1alpha1.OnecloudCluster, wantedZones []string, factory cloudComponentFactory) error {
	var zones = oc.GetZones()
	if len(wantedZones) > 0 {
		zones = []string{}
		for i := 0; i < len(wantedZones); i++ {
			if utils.IsInStringArray(wantedZones[i], oc.Spec.CustomZones) {
				zones = append(zones, wantedZones[i])
			}
		}
	}
	for _, zone := range zones {
		if zone == oc.Spec.Zone {
			// use empty string replace default zone
			zone = ""
		}
		if err := syncComponent(factory, oc, zone); err != nil {
			return err
		}
	}
	return nil
}

func (m *ComponentManager) getDBConfig(_ *v1alpha1.OnecloudClusterConfig) *v1alpha1.DBConfig {
	return nil
}

func (m *ComponentManager) getDBEngine(oc *v1alpha1.OnecloudCluster) v1alpha1.TDBEngineType {
	return ""
}

func (m *ComponentManager) getClickhouseConfig(_ *v1alpha1.OnecloudClusterConfig) *v1alpha1.DBConfig {
	return nil
}

func (m *ComponentManager) getCloudUser(_ *v1alpha1.OnecloudClusterConfig) *v1alpha1.CloudUser {
	return nil
}

func (m *ComponentManager) getPhaseControl(man controller.ComponentManager, zone string) controller.PhaseControl {
	return nil
}

func (m *ComponentManager) getDeploymentStatus(oc *v1alpha1.OnecloudCluster, zone string) *v1alpha1.DeploymentStatus {
	return nil
}

func (m *ComponentManager) getService(oc *v1alpha1.OnecloudCluster, cfg *v1alpha1.OnecloudClusterConfig, zone string) []*corev1.Service {
	return nil
}

func (m *ComponentManager) getIngress(oc *v1alpha1.OnecloudCluster, zone string) *unstructured.Unstructured {
	return nil
}

func (m *ComponentManager) updateIngress(_ *v1alpha1.OnecloudCluster, _ *unstructured.Unstructured) *unstructured.Unstructured {
	return nil
}

func (m *ComponentManager) getConfigMap(oc *v1alpha1.OnecloudCluster, cfg *v1alpha1.OnecloudClusterConfig, zone string) (*corev1.ConfigMap, bool, error) {
	return nil, false, nil
}

func (m *ComponentManager) getComponentManager() *ComponentManager {
	return m
}

func (m *ComponentManager) getPVC(oc *v1alpha1.OnecloudCluster, zone string) (*corev1.PersistentVolumeClaim, error) {
	return nil, nil
}
func (m *ComponentManager) getDeployment(oc *v1alpha1.OnecloudCluster, cfg *v1alpha1.OnecloudClusterConfig, zone string) (*apps.Deployment, error) {
	return nil, nil
}

func (m *ComponentManager) getDaemonSet(*v1alpha1.OnecloudCluster, *v1alpha1.OnecloudClusterConfig, string) (*apps.DaemonSet, error) {
	return nil, nil
}

func (m *ComponentManager) getCronJob(*v1alpha1.OnecloudCluster, *v1alpha1.OnecloudClusterConfig, string) (*batchv1.CronJob, error) {
	return nil, nil
}

func (m *ComponentManager) getZoneComponent(component v1alpha1.ComponentType, zone string) v1alpha1.ComponentType {
	if len(zone) == 0 {
		return component
	}
	return v1alpha1.ComponentType(fmt.Sprintf("%s-%s", component, zone))
}

func (m *ComponentManager) Etcd() manager.Manager {
	return newEtcdComponentManager(m)
}

func (m *ComponentManager) Keystone() manager.Manager {
	return newKeystoneComponentManager(m)
}

func (m *ComponentManager) Logger() manager.Manager {
	return newLoggerManager(m)
}

func (m *ComponentManager) Region() manager.Manager {
	return newRegionManager(m)
}

func (m *ComponentManager) RegionDNS() manager.Manager {
	return newRegionDNSManager(m)
}

func (m *ComponentManager) Climc() manager.Manager {
	return newClimcComponentManager(m)
}

func (m *ComponentManager) Cloudmux() manager.Manager {
	return newCloudmuxComponentManager(m)
}

func (m *ComponentManager) Glance() manager.Manager {
	return newGlanceManager(m)
}

func (m *ComponentManager) Webconsole() manager.Manager {
	return newWebconsoleManager(m)
}

func (m *ComponentManager) Scheduler() manager.Manager {
	return newSchedulerManager(m)
}

func (m *ComponentManager) APIMap() manager.Manager {
	return newAPIMapManager(m)
}

func (m *ComponentManager) Influxdb() manager.Manager {
	return newInfluxdbManager(m)
}

func (m *ComponentManager) VictoriaMetrics() manager.Manager {
	return newVictoriaMetricsManager(m)
}

func (m *ComponentManager) Yunionagent() manager.Manager {
	return newYunionagentManager(m)
}

func (m *ComponentManager) Yunionconf() manager.Manager {
	return newYunionconfManager(m)
}

func (m *ComponentManager) KubeServer() manager.Manager {
	return newKubeManager(m)
}

func (m *ComponentManager) AnsibleServer() manager.Manager {
	return newAnsibleManager(m)
}

func (m *ComponentManager) Cloudnet() manager.Manager {
	return newCloudnetManager(m)
}

func (m *ComponentManager) Cloudproxy() manager.Manager {
	return newCloudproxyManager(m)
}

func (m *ComponentManager) APIGateway() manager.Manager {
	return newAPIGatewayManager(m)
}

func (m *ComponentManager) Web() manager.Manager {
	return newWebManager(m)
}

func (m *ComponentManager) Notify() manager.Manager {
	return newNotifyManager(m)
}

func (m *ComponentManager) Baremetal() manager.Manager {
	return newBaremetalManager(m)
}

func (m *ComponentManager) OvnNorth() manager.Manager {
	return newOvnNorthManager(m)
}

func (m *ComponentManager) VpcAgent() manager.Manager {
	return newVpcAgentManager(m)
}

func (m *ComponentManager) Host() manager.Manager {
	return newHostManager(m)
}

func (m *ComponentManager) Lbgent() manager.Manager {
	return newLbagentManager(m)
}

func (m *ComponentManager) HostDeployer() manager.Manager {
	return newHostDeployerManger(m)
}

func (m *ComponentManager) HostImage() manager.Manager {
	return newHostImageManager(m)
}

func (m *ComponentManager) Cloudevent() manager.Manager {
	return newCloudeventManager(m)
}

func (m *ComponentManager) S3gateway() manager.Manager {
	return newS3gatewayManager(m)
}

func (m *ComponentManager) Devtool() manager.Manager {
	return newDevtoolManager(m)
}

func (m *ComponentManager) Meter() manager.Manager {
	return newMeterManager(m)
}

func (m *ComponentManager) Billing() manager.Manager {
	return newBillingManager(m)
}

func (m *ComponentManager) AutoUpdate() manager.Manager {
	return newAutoUpdateManager(m)
}

func (m *ComponentManager) EsxiAgent() manager.Manager {
	return newEsxiManager(m)
}

func (m *ComponentManager) Monitor() manager.Manager {
	return newMonitorManager(m)
}

func (m *ComponentManager) ServiceOperator() manager.Manager {
	return newServiceOperatorManager(m)
}

func (m *ComponentManager) Itsm() manager.Manager {
	return newItsmManager(m)
}

func (m *ComponentManager) Telegraf() manager.Manager {
	return newTelegrafManager(m)
}

func (m *ComponentManager) CloudId() manager.Manager {
	return newCloudIdManager(m)
}

func (m *ComponentManager) Cloudmon() manager.Manager {
	return newCloudMonManager(m)
}

func (m *ComponentManager) Suggestion() manager.Manager {
	return newSuggestionManager(m)
}

func (m *ComponentManager) Scheduledtask() manager.Manager {
	return newScheduledtaskManager(m)
}

func (m *ComponentManager) MonitorStack() manager.Manager {
	return newMonitorStackManager(m)
}

func (m *ComponentManager) Report() manager.Manager {
	return newReportManager(m)
}

func (m *ComponentManager) EChartsSSR() manager.Manager {
	return newEChartsSSR(m)
}

func (m *ComponentManager) HostHealth() manager.Manager {
	return newHostHealthManager(m)
}

func (m *ComponentManager) BastionHost() manager.Manager {
	return newBastionHostManager(m)
}

func (m *ComponentManager) Extdb() manager.Manager {
	return newExtdbManager(m)
}

func setSelfAntiAffnity(deploy *apps.Deployment, component v1alpha1.ComponentType) *apps.Deployment {
	if deploy.Spec.Template.Spec.Affinity == nil {
		deploy.Spec.Template.Spec.Affinity = new(corev1.Affinity)
	}
	deploy.Spec.Template.Spec.Affinity.PodAntiAffinity = &corev1.PodAntiAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
			{
				Weight: 100,
				PodAffinityTerm: corev1.PodAffinityTerm{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{label.AppLabelKey: component.String()},
					},
					TopologyKey: "kubernetes.io/hostname",
				},
			},
		},
	}
	return deploy
}

func (m *ComponentManager) shouldSyncConfigmap(
	oc *v1alpha1.OnecloudCluster,
	ct v1alpha1.ComponentType,
	opt interface{},
	cond func(string) bool,
) (*v1.ConfigMap, bool, error) {
	cfgMap := m.newServiceConfigMap(ct, "", oc, opt)
	cfgCli := m.kubeCli.CoreV1().ConfigMaps(oc.GetNamespace())

	oldCfgMap, _ := cfgCli.Get(context.Background(), cfgMap.GetName(), metav1.GetOptions{})
	if oldCfgMap != nil {
		optStr, ok := oldCfgMap.Data["config"]
		if ok {
			if cond(optStr) {
				return cfgMap, true, nil
			}
		}
	}
	return cfgMap, false, nil
}
