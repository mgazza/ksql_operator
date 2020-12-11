/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type ResourceStatus string

const (
	StatusApplied = "Applied"
	StatusPending = "Pending"
	StatusFailed  = "Failed"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ManagedKSQL is a specification for a ManagedKSQL resource
type ManagedKSQL struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Statement string            `json:"statement"`
	Status    ManagedKSQLStatus `json:"status"`
}

// ManagedKSQLStatus is the status for a ManagedKSQL resource
type ManagedKSQLStatus struct {
	Applied    ResourceStatus           `json:"applied"`
	ItemStatus map[string]CommandStatus `json:"itemStatus"`
}

type CommandStatus struct {
	CommandID string `json:"commandID"`
	QueryID   string `json:"queryID"`
	Status    Status `json:"status"`
	QuerySha  string `json:"querySha"`
	StatusSha string `json:"statusSha"`
}

type Status string

const (
	StatusQueued     = Status("QUEUED")
	StatusParsing    = Status("PARSING")
	StatusExecuting  = Status("EXECUTING")
	StatusTerminated = Status("TERMINATED")
	StatusSuccess    = Status("SUCCESS")
	StatusError      = Status("ERROR")
)

func ParseCommandStatus(status string) (Status, error) {
	switch Status(status) {
	case StatusQueued:
	case StatusParsing:
	case StatusExecuting:
	case StatusTerminated:
	case StatusSuccess:
	case StatusError:
	default:
		return "", fmt.Errorf("unknown status, %s", status)
	}

	return Status(status), nil
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ManagedKSQLList is a list of ManagedKSQL resources
type ManagedKSQLList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []ManagedKSQL `json:"items"`
}
