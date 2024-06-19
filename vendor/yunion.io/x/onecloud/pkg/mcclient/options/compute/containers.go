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

package compute

import (
	"os"
	"strconv"
	"strings"
	"time"

	"yunion.io/x/jsonutils"
	"yunion.io/x/pkg/errors"

	"yunion.io/x/onecloud/pkg/apis"
	computeapi "yunion.io/x/onecloud/pkg/apis/compute"
	"yunion.io/x/onecloud/pkg/mcclient/options"
)

type ContainerListOptions struct {
	options.BaseListOptions
	GuestId string `json:"guest_id" help:"guest(pod) id or name"`
}

func (o *ContainerListOptions) Params() (jsonutils.JSONObject, error) {
	return options.ListStructToParams(o)
}

type ContainerShowOptions struct {
	ServerIdOptions
}

type ContainerDeleteOptions struct {
	ServerIdsOptions
}

type ContainerCreateCommonOptions struct {
	IMAGE             string   `help:"Image of container" json:"image"`
	Command           []string `help:"Command to execute (i.e., entrypoint for docker)" json:"command"`
	Args              []string `help:"Args for the Command (i.e. command for docker)" json:"args"`
	WorkingDir        string   `help:"Current working directory of the command" json:"working_dir"`
	Env               []string `help:"List of environment variable to set in the container and the format is: <key>=<value>"`
	VolumeMount       []string `help:"Volume mount of the container and the format is: name=<val>,mount=<container_path>,readonly=<true_or_false>,disk_index=<disk_number>,disk_id=<disk_id>"`
	Device            []string `help:"Host device: <host_path>:<container_path>:<permissions>, e.g.: /dev/snd:/dev/snd:rwm"`
	Privileged        bool     `help:"Privileged mode"`
	Caps              string   `help:"Container capabilities, e.g.: SETPCAP,AUDIT_WRITE,SYS_CHROOT,CHOWN,DAC_OVERRIDE,FOWNER,SETGID,SETUID,SYSLOG,SYS_ADMIN,WAKE_ALARM,SYS_PTRACE,BLOCK_SUSPEND,MKNOD,KILL,SYS_RESOURCE,NET_RAW,NET_ADMIN,NET_BIND_SERVICE,SYS_NICE"`
	DropCaps          string   `help:"Container dropped capabilities, split by ','"`
	EnableLxcfs       bool     `help:"Enable lxcfs"`
	PostStartExec     string   `help:"Post started execution command"`
	CgroupDeviceAllow []string `help:"Cgroup devices.allow, e.g.: 'c 13:* rwm'"`
	SimulateCpu       bool     `help:"Simulating /sys/devices/system/cpu files"`
	ShmSizeMb         int      `help:"Shm size MB"`
	Uid               int64    `help:"UID of container" default:"0"`
	Gid               int64    `help:"GID of container" default:"0"`
}

func (o ContainerCreateCommonOptions) getCreateSpec() (*computeapi.ContainerSpec, error) {
	req := &computeapi.ContainerSpec{
		ContainerSpec: apis.ContainerSpec{
			Image:              o.IMAGE,
			Command:            o.Command,
			Args:               o.Args,
			WorkingDir:         o.WorkingDir,
			EnableLxcfs:        o.EnableLxcfs,
			Privileged:         o.Privileged,
			Capabilities:       &apis.ContainerCapability{},
			CgroupDevicesAllow: o.CgroupDeviceAllow,
			SimulateCpu:        o.SimulateCpu,
			SecurityContext: &apis.ContainerSecurityContext{
				RunAsUser:  nil,
				RunAsGroup: nil,
			},
		},
	}
	if o.ShmSizeMb > 0 {
		req.ContainerSpec.ShmSizeMB = o.ShmSizeMb
	}
	if o.Uid > 0 {
		req.ContainerSpec.SecurityContext.RunAsUser = &o.Uid
	}
	if o.Gid > 0 {
		req.ContainerSpec.SecurityContext.RunAsGroup = &o.Gid
	}
	if len(o.PostStartExec) != 0 {
		req.Lifecyle = &apis.ContainerLifecyle{
			PostStart: &apis.ContainerLifecyleHandler{
				Type: apis.ContainerLifecyleHandlerTypeExec,
				Exec: &apis.ContainerLifecyleHandlerExecAction{
					Command: strings.Split(o.PostStartExec, " "),
				},
			},
		}
	}
	if len(o.Caps) != 0 {
		req.Capabilities.Add = strings.Split(o.Caps, ",")
	}
	if len(o.DropCaps) != 0 {
		req.Capabilities.Drop = strings.Split(o.DropCaps, ",")
	}
	for _, env := range o.Env {
		e, err := parseContainerEnv(env)
		if err != nil {
			return nil, errors.Wrapf(err, "parseContainerEnv %s", env)
		}
		req.Envs = append(req.Envs, e)
	}
	for _, vmStr := range o.VolumeMount {
		vm, err := parseContainerVolumeMount(vmStr)
		if err != nil {
			return nil, errors.Wrapf(err, "parseContainerVolumeMount %s", vmStr)
		}
		req.VolumeMounts = append(req.VolumeMounts, vm)
	}
	devs := make([]*computeapi.ContainerDevice, len(o.Device))
	for idx, devStr := range o.Device {
		dev, err := parseContainerDevice(devStr)
		if err != nil {
			return nil, errors.Wrap(err, "parseContainerDevice")
		}
		devs[idx] = dev
	}
	req.Devices = devs
	return req, nil
}

type ContainerCreateOptions struct {
	ContainerCreateCommonOptions
	PODID string `help:"Name or id of server pod" json:"-"`
	NAME  string `help:"Name of container" json:"-"`
}

func (o *ContainerCreateOptions) Params() (jsonutils.JSONObject, error) {
	spec, err := o.getCreateSpec()
	if err != nil {
		return nil, errors.Wrap(err, "get container create spec")
	}
	req := computeapi.ContainerCreateInput{
		GuestId: o.PODID,
		Spec:    *spec,
	}
	req.Name = o.NAME
	return jsonutils.Marshal(req), nil
}

func parseContainerEnv(env string) (*apis.ContainerKeyValue, error) {
	kv := strings.Split(env, "=")
	if len(kv) != 2 {
		return nil, errors.Errorf("invalid env: %q", env)
	}
	return &apis.ContainerKeyValue{
		Key:   kv[0],
		Value: kv[1],
	}, nil
}

func parseContainerVolumeMount(vmStr string) (*apis.ContainerVolumeMount, error) {
	vm := &apis.ContainerVolumeMount{}
	for _, seg := range strings.Split(vmStr, ",") {
		info := strings.Split(seg, "=")
		if len(info) != 2 {
			return nil, errors.Errorf("invalid option %s", seg)
		}
		key := info[0]
		val := info[1]
		switch key {
		case "read_only", "ro", "readonly":
			if strings.ToLower(val) == "true" {
				vm.ReadOnly = true
			}
		case "fs_user":
			uId, err := strconv.Atoi(val)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid fs_user %s", val)
			}
			uId64 := int64(uId)
			vm.FsUser = &uId64
		case "fs_group":
			gId, err := strconv.Atoi(val)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid fs_group %s", val)
			}
			gId64 := int64(gId)
			vm.FsGroup = &gId64
		case "mount_path":
			vm.MountPath = val
		case "host_path":
			if vm.HostPath == nil {
				vm.HostPath = &apis.ContainerVolumeMountHostPath{}
			}
			vm.Type = apis.CONTAINER_VOLUME_MOUNT_TYPE_HOST_PATH
			vm.HostPath.Path = val
		case "host_type":
			if vm.HostPath == nil {
				vm.HostPath = &apis.ContainerVolumeMountHostPath{}
			}
			vm.HostPath.Type = apis.ContainerVolumeMountHostPathType(val)
		case "disk_index":
			vm.Type = apis.CONTAINER_VOLUME_MOUNT_TYPE_DISK
			if vm.Disk == nil {
				vm.Disk = &apis.ContainerVolumeMountDisk{}
			}
			index, err := strconv.Atoi(val)
			if err != nil {
				return nil, errors.Wrapf(err, "wrong disk_index %s", val)
			}
			vm.Disk.Index = &index
		case "disk_id":
			vm.Type = apis.CONTAINER_VOLUME_MOUNT_TYPE_DISK
			if vm.Disk == nil {
				vm.Disk = &apis.ContainerVolumeMountDisk{}
			}
			vm.Disk.Id = val
		case "disk_subdir", "disk_sub_dir", "disk_sub_directory":
			vm.Type = apis.CONTAINER_VOLUME_MOUNT_TYPE_DISK
			if vm.Disk == nil {
				vm.Disk = &apis.ContainerVolumeMountDisk{}
			}
			vm.Disk.SubDirectory = val
		case "disk_storage_size_file", "disk_ssf":
			vm.Type = apis.CONTAINER_VOLUME_MOUNT_TYPE_DISK
			if vm.Disk == nil {
				vm.Disk = &apis.ContainerVolumeMountDisk{}
			}
			vm.Disk.StorageSizeFile = val
		case "overlay":
			if vm.Disk == nil {
				vm.Disk = &apis.ContainerVolumeMountDisk{}
			}
			vm.Disk.Overlay = &apis.ContainerVolumeMountDiskOverlay{
				LowerDir: strings.Split(val, ":"),
			}
		case "overlay_disk_image":
			if strings.ToLower(val) == "true" {
				if vm.Disk == nil {
					vm.Disk = &apis.ContainerVolumeMountDisk{}
				}
				vm.Disk.Overlay = &apis.ContainerVolumeMountDiskOverlay{
					UseDiskImage: true,
				}
			}
		case "text_file":
			content, err := os.ReadFile(val)
			if err != nil {
				return nil, errors.Wrapf(err, "read file %s", val)
			}
			vm.Type = apis.CONTAINER_VOLUME_MOUNT_TYPE_TEXT
			vm.Text = &apis.ContainerVolumeMountText{
				Content: string(content),
			}
		}
	}
	return vm, nil
}

type ContainerIdsOptions struct {
	ID []string `help:"ID of containers to operate" metavar:"CONTAINER" json:"-"`
}

func (o *ContainerIdsOptions) GetIds() []string {
	return o.ID
}

func (o *ContainerIdsOptions) Params() (jsonutils.JSONObject, error) {
	return nil, nil
}

type ContainerStopOptions struct {
	ContainerIdsOptions
	Timeout int `help:"Stopping timeout" json:"timeout"`
}

func (o *ContainerStopOptions) Params() (jsonutils.JSONObject, error) {
	return jsonutils.Marshal(o), nil
}

type ContainerStartOptions struct {
	ContainerIdsOptions
}

type ContainerSaveVolumeMountImage struct {
	options.ResourceIdOptions
	IMAGENAME    string `help:"Image name"`
	INDEX        int    `help:"Index of volume mount"`
	GenerateName string `help:"Generate image name automatically"`
	Notes        string `help:"Extra notes of the image"`
}

func (o ContainerSaveVolumeMountImage) Params() (jsonutils.JSONObject, error) {
	return jsonutils.Marshal(&computeapi.ContainerSaveVolumeMountToImageInput{
		Name:         o.IMAGENAME,
		GenerateName: o.GenerateName,
		Notes:        o.Notes,
		Index:        o.INDEX,
	}), nil
}

type ContainerExecOptions struct {
	ServerIdOptions
	// Tty     bool `help:"Using tty" short-token:"t"`
	COMMAND string
	Args    []string
}

func (o *ContainerExecOptions) ToAPIInput() *computeapi.ContainerExecInput {
	cmd := []string{o.COMMAND}
	cmd = append(cmd, o.Args...)
	return &computeapi.ContainerExecInput{
		Command: cmd,
		//Tty:     o.Tty,
		Tty: true,
	}
}

func (o *ContainerExecOptions) Params() (jsonutils.JSONObject, error) {
	return jsonutils.Marshal(o.ToAPIInput()), nil
}

type ContainerExecSyncOptions struct {
	ServerIdOptions
	COMMAND string
	Args    []string
	Timeout int64
}

func (o *ContainerExecSyncOptions) Params() (jsonutils.JSONObject, error) {
	cmd := []string{o.COMMAND}
	cmd = append(cmd, o.Args...)
	return jsonutils.Marshal(&computeapi.ContainerExecSyncInput{
		Command: cmd,
		Timeout: o.Timeout,
	}), nil
}

type ContainerLogOptions struct {
	ServerIdOptions
	Since      string `help:"Only return logs newer than a relative duration like 5s, 2m, or 3h" json:"since"`
	Follow     bool   `help:"Follow log output" short-token:"f" json:"follow"`
	Tail       int64  `help:"Lines of recent log file to display" json:"tail"`
	Timestamps bool   `help:"Show timestamps on each line in the log output" json:"timestamps"`
	LimitBytes int64  `help:"Maximum amount of bytes that can be used." json:"limitBytes"`
}

func (o *ContainerLogOptions) Params() (jsonutils.JSONObject, error) {
	input, err := o.ToAPIInput()
	if err != nil {
		return nil, err
	}
	return jsonutils.Marshal(input), nil
}

func (o *ContainerLogOptions) ToAPIInput() (*computeapi.PodLogOptions, error) {
	opt := &computeapi.PodLogOptions{
		Follow:     o.Follow,
		Timestamps: o.Timestamps,
	}
	if o.LimitBytes > 0 {
		opt.LimitBytes = &o.LimitBytes
	}
	if o.Tail > 0 {
		opt.TailLines = &o.Tail
	}
	if len(o.Since) > 0 {
		dur, err := time.ParseDuration(o.Since)
		if err != nil {
			return nil, errors.Wrapf(err, "parse duration %s", o.Since)
		}
		sec := int64(dur.Round(time.Second).Seconds())
		opt.SinceSeconds = &sec
	}
	return opt, nil
}
