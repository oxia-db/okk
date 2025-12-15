package v1

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	TestCaseTypeBasicKv                  = "basic"
	TestCaseTypeSecondaryIndex           = "secondaryIndex"
	TestCaseTypeStreamingSequence        = "streamingSequence"
	TestCaseTypeMetadataWithEphemeral    = "metadataWithEphemeral"
	TestCaseTypeMetadataWithVersionId    = "metadataWithVersionId"
	TestCaseTypeMetadataWithNotification = "notification"
)

type TestCaseSpec struct {
	//+kubebuilder:validation:Required
	Type string `json:"type"`
	//+kubebuilder:validation:Required
	Worker Worker `json:"worker,omitempty"`

	OpPerSec *int `json:"opPerSec,omitempty"`

	Duration *time.Duration `json:"duration,omitempty"`
}

type TestCaseStatus struct {
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// TestCase is the Schema for the tcmetadataephemerals API.
type TestCase struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TestCaseSpec   `json:"spec,omitempty"`
	Status TestCaseStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type TCMetadataEphemeralList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TestCase `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TestCase{}, &TCMetadataEphemeralList{})
}
