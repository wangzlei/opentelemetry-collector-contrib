// Package metadata provides component metadata for the genaiadapterconnector.
package metadata

import (
	"go.opentelemetry.io/collector/component"
)

// Component metadata.
var (
	Type      = component.MustNewType("genaiadapterconnector")
	Stability = component.StabilityLevelDevelopment
)
