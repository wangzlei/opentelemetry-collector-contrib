package metadata

import (
	"go.opentelemetry.io/collector/component"
)

var (
	Type           = component.MustNewType("largelogobject")
	LogsStability  = component.StabilityLevelDevelopment
)
