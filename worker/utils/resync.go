package utils

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
)

type ResyncTarget struct {
	Namespace string
	Table     string
	ID        string
}

// ResyncTargetFromRequest keeps the CLI selector and retry identity authoritative.
func ResyncTargetFromRequest(req *types.ExecutionRequest) (ResyncTarget, error) {
	var target ResyncTarget
	if req.Command != types.Resync || len(req.Args) == 0 || req.Args[0] != string(types.Resync) {
		return target, fmt.Errorf("resync requires the dedicated resync command")
	}
	values := map[string]*string{
		constants.ResyncNamespaceFlag: &target.Namespace,
		constants.ResyncTableFlag:     &target.Table,
		constants.ResyncIDFlag:        &target.ID,
	}
	seen := make(map[string]bool, len(values))
	for i := 1; i < len(req.Args); i++ {
		flag, value, inline := strings.Cut(req.Args[i], "=")
		dest, ok := values[flag]
		if !ok {
			continue
		}
		if seen[flag] {
			return target, fmt.Errorf("duplicate resync flag %s", flag)
		}
		seen[flag] = true
		if !inline {
			i++
			if i >= len(req.Args) || strings.HasPrefix(req.Args[i], "--") {
				return target, fmt.Errorf("missing value for %s", flag)
			}
			value = req.Args[i]
		}
		if strings.TrimSpace(value) == "" {
			return target, fmt.Errorf("blank value for %s", flag)
		}
		*dest = value
	}
	for flag := range values {
		if !seen[flag] {
			return target, fmt.Errorf("missing required resync flag %s", flag)
		}
	}
	if target.ID != req.WorkflowID || strings.TrimSpace(req.WorkflowID) == "" {
		return target, fmt.Errorf("resync id must match the stable workflow id")
	}
	return target, nil
}
