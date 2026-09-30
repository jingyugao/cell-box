// SPDX-License-Identifier: Apache-2.0

package main

import (
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

func clientNew(m manager.Manager) (client.Client, error) {
	return client.New(m.GetConfig(), client.Options{Scheme: m.GetScheme()})
}
