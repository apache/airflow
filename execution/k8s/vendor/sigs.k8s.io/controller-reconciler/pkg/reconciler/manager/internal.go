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

package manager

import ()

// Add Register resource manager
func (rm *ResourceManager) Add(rsrcType string, m Manager) {
	if rm.rsrcmgrs == nil {
		rm.rsrcmgrs = make(map[string]Manager)
	}
	rm.rsrcmgrs[rsrcType] = m
}

// Get  resource manager
func (rm *ResourceManager) Get(rsrcType string) Manager {
	m, ok := rm.rsrcmgrs[rsrcType]
	if ok {
		return m
	}
	return nil
}

// All returns all managers
func (rm *ResourceManager) All() map[string]Manager {
	return rm.rsrcmgrs
}
