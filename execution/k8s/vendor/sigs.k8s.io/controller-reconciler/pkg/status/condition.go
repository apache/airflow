/*
Copyright 2018 Google LLC
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

package status

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (m *Meta) addCondition(ctype ConditionType, status corev1.ConditionStatus, reason, message string) {
	now := metav1.Now()
	c := &Condition{
		Type:               ctype,
		LastUpdateTime:     now,
		LastTransitionTime: now,
		Status:             status,
		Reason:             reason,
		Message:            message,
	}
	//fmt.Printf(" <>>>>> adding ocndition: %s\n", ctype)
	m.Conditions = append(m.Conditions, *c)
}

// setConditionValue updates or creates a new condition
func (m *Meta) setConditionValue(ctype ConditionType, status corev1.ConditionStatus, reason, message string) {
	var c *Condition
	for i := range m.Conditions {
		if m.Conditions[i].Type == ctype {
			c = &m.Conditions[i]
		}
	}
	if c == nil {
		m.addCondition(ctype, status, reason, message)
	} else {
		// check message ?
		if c.Status == status && c.Reason == reason && c.Message == message {
			return
		}
		now := metav1.Now()
		c.LastUpdateTime = now
		if c.Status != status {
			c.LastTransitionTime = now
		}
		c.Status = status
		c.Reason = reason
		c.Message = message
	}
}

// RemoveCondition removes the condition with the provided type.
func (m *Meta) RemoveCondition(ctype ConditionType) {
	for i, c := range m.Conditions {
		if c.Type == ctype {
			m.Conditions[i] = m.Conditions[len(m.Conditions)-1]
			m.Conditions = m.Conditions[:len(m.Conditions)-1]
			break
		}
	}
}

// GetCondition get existing condition
func (m *Meta) GetCondition(ctype ConditionType) *Condition {
	for i := range m.Conditions {
		if m.Conditions[i].Type == ctype {
			return &m.Conditions[i]
		}
	}
	return nil
}

// IsConditionTrue - if condition is true
func (m *Meta) IsConditionTrue(ctype ConditionType) bool {
	if c := m.GetCondition(ctype); c != nil {
		return c.Status == corev1.ConditionTrue
	}
	return false
}

// IsReady returns true if ready condition is set
func (m *Meta) IsReady() bool { return m.IsConditionTrue(Ready) }

// IsNotReady returns true if ready condition is set
func (m *Meta) IsNotReady() bool { return !m.IsConditionTrue(Ready) }

// ConditionReason - return condition reason
func (m *Meta) ConditionReason(ctype ConditionType) string {
	if c := m.GetCondition(ctype); c != nil {
		return c.Reason
	}
	return ""
}

// Ready - shortcut to set ready contition to true
func (m *Meta) Ready(reason, message string) {
	m.SetCondition(Ready, reason, message)
}

// NotReady - shortcut to set ready contition to false
func (m *Meta) NotReady(reason, message string) {
	m.ClearCondition(Ready, reason, message)
}

// SetError - shortcut to set error condition
func (m *Meta) SetError(reason, message string) {
	m.SetCondition(Error, reason, message)
}

// ClearError - shortcut to set error condition
func (m *Meta) ClearError() {
	m.ClearCondition(Error, "NoError", "No error seen")
}

// Settled - shortcut to set Settled contition to true
func (m *Meta) Settled(reason, message string) {
	m.SetCondition(Settled, reason, message)
}

// NotSettled - shortcut to set Settled contition to false
func (m *Meta) NotSettled(reason, message string) {
	m.ClearCondition(Settled, reason, message)
}

// EnsureCondition useful for adding default conditions
func (m *Meta) EnsureCondition(ctype ConditionType) {
	if c := m.GetCondition(ctype); c != nil {
		return
	}
	m.addCondition(ctype, corev1.ConditionUnknown, ReasonInit, "Not Observed")
}

// EnsureStandardConditions - helper to inject standard conditions
func (m *Meta) EnsureStandardConditions() {
	m.EnsureCondition(Ready)
	m.EnsureCondition(Settled)
	m.EnsureCondition(Error)
}

// ClearCondition updates or creates a new condition
func (m *Meta) ClearCondition(ctype ConditionType, reason, message string) {
	m.setConditionValue(ctype, corev1.ConditionFalse, reason, message)
}

// SetCondition updates or creates a new condition
func (m *Meta) SetCondition(ctype ConditionType, reason, message string) {
	m.setConditionValue(ctype, corev1.ConditionTrue, reason, message)
}

// RemoveAllConditions updates or creates a new condition
func (m *Meta) RemoveAllConditions() {
	m.Conditions = []Condition{}
}

// ClearAllConditions updates or creates a new condition
func (m *Meta) ClearAllConditions() {
	for i := range m.Conditions {
		m.Conditions[i].Status = corev1.ConditionFalse
	}
}
