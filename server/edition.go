package server

import (
	"errors"
	redis "github.com/adbc-drivers/redis/go"
)

var errFeatureUnavailable = errors.New("requested enterprise feature is unavailable in this build")

const enterpriseBuild = false

// FeatureCapabilities reports implementation availability, not health or entitlement.
func FeatureCapabilities() map[string]bool {
	return map[string]bool{"enterprise": enterpriseBuild, "active_active": false, "migration": false, "monitoring": enterpriseBuild, "optimization": enterpriseBuild, "security": enterpriseBuild, "automation": enterpriseBuild, "backup": false}
}

func validateEnterpriseDriver() error {
	if enterpriseBuild && !redis.FeatureCapabilities()["enterprise"] {
		return errFeatureUnavailable
	}
	return nil
}
