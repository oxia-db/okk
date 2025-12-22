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

	OpRate *int `json:"opRate,omitempty"`

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

func (tc *TestCase) Duration() *time.Duration {
	var d = 10 * time.Minute
	if tc.Spec.Duration != nil {
		d = *tc.Spec.Duration
	}
	return &d
}

func (tc *TestCase) OpRate() int {
	ops := 10
	if tc.Spec.OpRate != nil && *tc.Spec.OpRate > 0 {
		ops = *tc.Spec.OpRate
	}
	return ops
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
