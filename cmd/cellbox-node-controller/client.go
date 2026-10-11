// SPDX-License-Identifier: Apache-2.0

package main

import (
	"cellbox.local/cellbox/internal/telemetry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

func clientNew(m manager.Manager) (client.Client, error) {
	c, err := client.New(m.GetConfig(), client.Options{Scheme: m.GetScheme()})
	if err != nil {
		return nil, err
	}
	return telemetry.TraceClient(c), nil
}
