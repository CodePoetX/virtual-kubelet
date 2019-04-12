package openstack

import (
	zun_container "github.com/gophercloud/gophercloud/openstack/container/v1/container"
)

type GetPodResult struct {
	NamespaceAndName string
	ContainerID      string
	Podinfo          *PodOTemplate
}

type PodDB map[string]*GetPodResult

// CreateOpts represents options used to create a network.
type PodOTemplate struct {
	Spec     PodSpec  `json:"spec,omitempty"`
	Kind     string   `json:"kind,omitempty"`
	Metadata Metadata `json:"metadata,omitempty"`
}

type PodSpec struct {
	Volumes       []Volume                   `json:"volumes,omitempty"`
	Containers    []*zun_container.Container `json:"containers,omitempty"`
	RestartPolicy string                     `json:"restartPolicy,omitempty"`
}

type Metadata struct {
	Labels map[string]string `json:"labels,omitempty"`
	Name   string            `json:"name,omitempty"`
}

type Volume struct {
	Name string `json:"name,omitempty"`
}
