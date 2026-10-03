package memory

import (
	"testing"

	"github.com/udaykishore-resu/strata/internal/state"
	"github.com/udaykishore-resu/strata/internal/store/storetest"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(*testing.T) state.Store { return New() })
}
