package openstack

import (
	"context"
	"fmt"
	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack"
	zun_container "github.com/gophercloud/gophercloud/openstack/container/v1/container"
	"github.com/gophercloud/gophercloud/pagination"
	"github.com/virtual-kubelet/virtual-kubelet/manager"
	"github.com/virtual-kubelet/virtual-kubelet/providers"
	"io"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/remotecommand"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

var podsDB = make(PodDB, 40)

// ZunProvider implements the virtual-kubelet provider interface and communicates with OpenStack's Zun APIs.
type ZunProvider struct {
	ZunClient              *gophercloud.ServiceClient
	ContainerNetworkClient *gophercloud.ServiceClient
	resourceManager        *manager.ResourceManager
	region                 string
	nodeName               string
	operatingSystem        string
	cpu                    string
	memory                 string
	pods                   string
	daemonEndpointPort     int32
}

// NewZunProvider creates a new ZunProvider.
func NewZunProvider(config string, rm *manager.ResourceManager, nodeName string, operatingSystem string, daemonEndpointPort int32) (*ZunProvider, error) {
	var p ZunProvider
	var err error

	p.resourceManager = rm

	AuthOptions, err := openstack.AuthOptionsFromEnv()

	if err != nil {
		return nil, fmt.Errorf("Unable to get the Auth options from environment variables: %s ", err)
	}

	Provider, err := openstack.AuthenticatedClient(AuthOptions)

	if err != nil {
		return nil, fmt.Errorf("Unable to get provider: %s ", err)
	}

	p.ZunClient, err = openstack.NewContainerV1(Provider, gophercloud.EndpointOpts{
		Region: os.Getenv("OS_REGION_NAME"),
	})

	if err != nil {
		return nil, fmt.Errorf("Unable to get zun client ")
	}

	p.ContainerNetworkClient, err = openstack.NewNetworkV2(Provider, gophercloud.EndpointOpts{
		Region: os.Getenv("OS_REGION_NAME"),
	})

	if err != nil {
		return nil, fmt.Errorf("Unable to get zuncontainernetwork client ")
	}
	// Set sane defaults for Capacity in case config is not supplied
	p.cpu = "24"
	p.memory = "64Gi"
	p.pods = "20"
	p.operatingSystem = operatingSystem
	p.nodeName = nodeName
	p.daemonEndpointPort = daemonEndpointPort
	p.ZunClient.Microversion = "1.9"
	return &p, err
}

// CreatePod takes a Kubernetes Pod and deploys it within the provider.
func (p *ZunProvider) CreatePod(ctx context.Context, pod *v1.Pod) error {
	//pod spec info
	var podTemplate = new(PodOTemplate)
	podTemplate.Kind = "Zun_Pod"
	podUID := string(pod.UID)
	podCreationTimestamp := pod.CreationTimestamp.String()
	var metadata Metadata
	metadata.Labels = map[string]string{
		"PodName":           pod.Name,
		"ClusterName":       pod.ClusterName,
		"NodeName":          pod.Spec.NodeName,
		"Namespace":         pod.Namespace,
		"UID":               podUID,
		"CreationTimestamp": podCreationTimestamp,
	}

	// create container info
	metadata.Name = pod.Namespace + "-" + pod.Name
	podTemplate.Metadata = metadata
	createOpts, err := CreateZunContainerOpts(pod)
	if err != nil {
		return fmt.Errorf("CreateZunContainerOpts function is error + %s ", err)
	}
	result, err := zun_container.Create(p.ZunClient, createOpts)
	if err != nil {
		return fmt.Errorf("zun_container.Create is error : %s ", err)
	}
	container, err := result.Extract()
	if err != nil {
		return fmt.Errorf("result.Extract() is error : %s ", err)
	}
	nn := fmt.Sprintf("%s-%s", pod.Namespace, pod.Name)
	if _, ok := podsDB[nn]; !ok {
		podsDB[nn] = &GetPodResult{
			NamespaceAndName: nn,
			ContainerID:      container.UUID,
			Podinfo:          podTemplate,
		}
	}

	return err
}

func CreateZunContainerOpts(pod *v1.Pod) (createOpts zun_container.CreateOpts, err error) {
	if pod == nil {
		err = fmt.Errorf("Pod is null.CreateZunContainerOpts function in zun ")
		return
	}

	for _, container := range pod.Spec.Containers {
		// get pod name
		podName := fmt.Sprintf("%s-%s", pod.Name, container.Name)
		if podName != "" {
			createOpts.Name = podName
		}

		//get pod image
		createOpts.Image = container.Image

		isInteractive := true
		createOpts.Interactive = &isInteractive

		//get pod env
		env := make(map[string]string, len(container.Env))
		for _, v := range container.Env {
			env[v.Name] = v.Value
		}
		createOpts.Environment = env

		//get pod labels
		createOpts.Labels = pod.Labels

		//get work dir
		createOpts.Workdir = container.WorkingDir

		//get image pull policy
		createOpts.ImagePullPolicy = strings.ToLower(string(container.ImagePullPolicy))

		//get pod command
		command := ""
		if len(container.Command) > 0 {
			for _, v := range container.Command {
				command = command + v + " "
			}
		}
		if len(container.Args) > 0 {
			for _, v := range container.Args {
				command = command + v + " "
			}
		}
		if command != "" {
			createOpts.Command = command
		}

		//get pod resource

		if container.Resources.Limits != nil {
			cpuLimit := float64(1)
			if _, ok := container.Resources.Limits[v1.ResourceCPU]; ok {
				cpuLimit = float64(container.Resources.Limits.Cpu().MilliValue()) / 1000.00
			}

			memoryLimit := 0.5
			if _, ok := container.Resources.Limits[v1.ResourceMemory]; ok {
				memoryLimit = float64(container.Resources.Limits.Memory().Value()) / (1024 * 1024)
			}

			createOpts.Cpu = cpuLimit
			createOpts.Memory = int(memoryLimit)
		} else if container.Resources.Requests != nil {
			cpuRequests := float64(1)
			if _, ok := container.Resources.Requests[v1.ResourceCPU]; ok {
				cpuRequests = float64(container.Resources.Requests.Cpu().MilliValue()) / 1000.00
			}

			memoryRequests := 0.5
			if _, ok := container.Resources.Requests[v1.ResourceMemory]; ok {
				memoryRequests = float64(container.Resources.Requests.Memory().Value()) / (1024 * 1024)
			}

			createOpts.Cpu = cpuRequests
			createOpts.Memory = int(memoryRequests)
		}

	}
	return createOpts, nil
}

// UpdatePod takes a Kubernetes Pod and updates it within the provider.
func (p *ZunProvider) UpdatePod(ctx context.Context, pod *v1.Pod) error {
	return nil
}

// DeletePod takes a Kubernetes Pod and deletes it from the provider.
func (p *ZunProvider) DeletePod(ctx context.Context, pod *v1.Pod) error {
	nn := fmt.Sprintf("%s-%s", pod.Namespace, pod.Name)
	if v, ok := podsDB[nn]; ok {
		err := zun_container.Delete(p.ZunClient, v.ContainerID, true).ExtractErr()
		if err != nil {
			return err
		}
		delete(podsDB, nn)
		return nil
	}
	return fmt.Errorf("Delete Pod is fail, pod is not found! ")
}

// GetPod retrieves a pod by name from the provider (can be cached).
func (p *ZunProvider) GetPod(ctx context.Context, namespace, name string) (*v1.Pod, error) {
	nn := fmt.Sprintf("%s-%s", namespace, name)
	if v, ok := podsDB[nn]; ok {
		container, err := zun_container.Get(p.ZunClient, v.ContainerID).Extract()
		if err != nil {
			return nil, fmt.Errorf("zun_container.Get(p.ZunClient,v.ContainerID).Extract() is error : %s ", err)
		}
		return containerToPod(container, v.Podinfo)
	}

	return nil, fmt.Errorf("get pod is fail,pod not found")
}

func containerToPod(c *zun_container.Container, podInfo *PodOTemplate) (pod *v1.Pod, err error) {
	containers := make([]v1.Container, 1)
	containerStatuses := make([]v1.ContainerStatus, 1)
	containerMemoryMB := 0
	if c.Memory != "" {
		containerMemory, err := strconv.Atoi(c.Memory)
		if err != nil {
			log.Println(err)
		}
		containerMemoryMB = containerMemory
	}
	container := v1.Container{
		Name:    c.Name,
		Image:   c.Image,
		Command: c.Command,
		Resources: v1.ResourceRequirements{
			Limits: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse(fmt.Sprintf("%g", float64(c.CPU))),
				v1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dM", containerMemoryMB)),
			},

			Requests: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse(fmt.Sprintf("%g", float64(c.CPU*1024/100))),
				v1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dM", containerMemoryMB)),
			},
		},
	}
	containers = append(containers, container)
	containerStatus := v1.ContainerStatus{
		Name:                 c.Name,
		State:                zunContainerStausToContainerStatus(c),
		LastTerminationState: zunContainerStausToContainerStatus(c),
		Ready:                zunStatusToPodPhase(c.Status) == v1.PodRunning,
		RestartCount:         int32(0),
		Image:                c.Image,
		ImageID:              "",
		ContainerID:          c.UUID,
	}

	// Add to containerStatuses
	containerStatuses = append(containerStatuses, containerStatus)

	var containerStartTime metav1.Time
	containerStartTime = metav1.NewTime(time.Time{})
	p := v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              podInfo.Metadata.Labels["PodName"],
			Namespace:         podInfo.Metadata.Labels["Namespace"],
			ClusterName:       podInfo.Metadata.Labels["ClusterName"],
			UID:               types.UID(podInfo.Metadata.Labels["UID"]),
			CreationTimestamp: metav1.NewTime(time.Now()),
		},
		Spec: v1.PodSpec{
			NodeName:   "virtual-kubelet",
			Volumes:    []v1.Volume{},
			Containers: containers,
		},
		Status: v1.PodStatus{
			Phase:             zunStatusToPodPhase(c.Status),
			Conditions:        []v1.PodCondition{},
			Message:           "",
			Reason:            "",
			HostIP:            "",
			PodIP:             c.GetIp(),
			StartTime:         &containerStartTime,
			ContainerStatuses: containerStatuses,
		},
	}
	return &p, nil
}

// GetContainerLogs retrieves the logs of a container by name from the provider.
func (p *ZunProvider) GetContainerLogs(ctx context.Context, namespace, podName, containerName string, tail int) (string, error) {
	return "not support in Zun Provider", nil
}

// ExecInContainer executes a command in a container in the pod, copying data
// between in/out/err and the container's stdin/stdout/stderr.
func (p *ZunProvider) ExecInContainer(name string, uid types.UID, container string, cmd []string, in io.Reader, out, err io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize, timeout time.Duration) error {
	log.Printf("receive ExecInContainer %q\n", container)
	return nil
}

// GetPodStatus retrieves the status of a pod by name from the provider.
func (p *ZunProvider) GetPodStatus(ctx context.Context, namespace, name string) (*v1.PodStatus, error) {
	pod, err := p.GetPod(ctx, namespace, name)
	if err != nil {
		return nil, err
	}

	if pod == nil {
		return nil, nil
	}

	return &pod.Status, nil
}

// GetPods retrieves a list of all pods running on the provider (can be cached).
func (p *ZunProvider) GetPods(context.Context) ([]*v1.Pod, error) {
	pager := zun_container.List(p.ZunClient, nil)
	pages := 0
	err := pager.EachPage(func(page pagination.Page) (bool, error) {
		pages++
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	pods := make([]*v1.Pod, 0, pages)
	err = pager.EachPage(func(page pagination.Page) (bool, error) {
		containerList, err := zun_container.ExtractContainers(page)
		if err != nil {
			return false, err
		}

		for _, m := range containerList {
			c := m
			temp := new(PodOTemplate)
			for _, v := range podsDB {
				if v.ContainerID == c.UUID {
					temp = v.Podinfo
				}
			}
			p, err := containerToPod(&c, temp)
			if err != nil {
				log.Println(err)
				continue
			}
			pods = append(pods, p)
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return pods, nil

}

// Capacity returns a resource list with the capacity constraints of the provider.
func (p *ZunProvider) Capacity(context.Context) v1.ResourceList {
	return v1.ResourceList{
		"cpu":    resource.MustParse(p.cpu),
		"memory": resource.MustParse(p.memory),
		"pods":   resource.MustParse(p.pods),
	}
}

// NodeConditions returns a list of conditions (Ready, OutOfDisk, etc), which is
// polled periodically to update the node status within Kubernetes.
func (p *ZunProvider) NodeConditions(context.Context) []v1.NodeCondition {
	return []v1.NodeCondition{
		{
			Type:               "Ready",
			Status:             v1.ConditionTrue,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletReady",
			Message:            "kubelet is ready.",
		},
		{
			Type:               "OutOfDisk",
			Status:             v1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletHasSufficientDisk",
			Message:            "kubelet has sufficient disk space available",
		},
		{
			Type:               "MemoryPressure",
			Status:             v1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletHasSufficientMemory",
			Message:            "kubelet has sufficient memory available",
		},
		{
			Type:               "DiskPressure",
			Status:             v1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletHasNoDiskPressure",
			Message:            "kubelet has no disk pressure",
		},
		{
			Type:               "NetworkUnavailable",
			Status:             v1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "RouteCreated",
			Message:            "RouteController created a route",
		},
	}
}

// NodeAddresses returns a list of addresses for the node status
// within Kubernetes.
func (p *ZunProvider) NodeAddresses(context.Context) []v1.NodeAddress {
	return nil
}

// NodeDaemonEndpoints returns NodeDaemonEndpoints for the node status
// within Kubernetes.
func (p *ZunProvider) NodeDaemonEndpoints(context.Context) *v1.NodeDaemonEndpoints {
	return &v1.NodeDaemonEndpoints{
		KubeletEndpoint: v1.DaemonEndpoint{
			Port: p.daemonEndpointPort,
		},
	}
}

// OperatingSystem returns the operating system the provider is for.
func (p *ZunProvider) OperatingSystem() string {
	if p.operatingSystem != "" {
		return p.operatingSystem
	}
	return providers.OperatingSystemLinux
}

func zunContainerStausToContainerStatus(cs *zun_container.Container) v1.ContainerState {
	// Zun already container start time but not add support at gophercloud
	//startTime := metav1.NewTime(time.Time(cs.StartTime))

	// Zun container status:
	//'Error', 'Running', 'Stopped', 'Paused', 'Unknown', 'Creating', 'Created',
	//'Deleted', 'Deleting', 'Rebuilding', 'Dead', 'Restarting'

	// Handle the case where the container is running.
	if cs.Status == "Running" || cs.Status == "Stopped" {
		return v1.ContainerState{
			Running: &v1.ContainerStateRunning{
				StartedAt: metav1.NewTime(time.Time(time.Now())),
			},
		}
	}

	// Handle the case where the container failed.
	if cs.Status == "Error" || cs.Status == "Dead" {
		return v1.ContainerState{
			Terminated: &v1.ContainerStateTerminated{
				ExitCode:   int32(0),
				Reason:     cs.Status,
				Message:    cs.StatusDetail,
				StartedAt:  metav1.NewTime(time.Time(time.Now())),
				FinishedAt: metav1.NewTime(time.Time(time.Now())),
			},
		}
	}

	// Handle the case where the container is pending.
	// Which should be all other Zun states.
	return v1.ContainerState{
		Waiting: &v1.ContainerStateWaiting{
			Reason:  cs.Status,
			Message: cs.StatusDetail,
		},
	}
}

func zunStatusToPodPhase(status string) v1.PodPhase {
	switch status {
	case "Running":
		return v1.PodRunning
	case "Stopped":
		return v1.PodSucceeded
	case "Error":
		return v1.PodFailed
	case "Dead":
		return v1.PodFailed
	case "Creating":
		return v1.PodPending
	case "Created":
		return v1.PodPending
	case "Restarting":
		return v1.PodPending
	case "Rebuilding":
		return v1.PodPending
	case "Paused":
		return v1.PodPending
	case "Deleting":
		return v1.PodPending
	case "Deleted":
		return v1.PodPending
	}

	return v1.PodUnknown
}
