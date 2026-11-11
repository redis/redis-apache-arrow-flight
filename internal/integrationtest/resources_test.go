package integrationtest

import (
	"slices"
	"testing"
)

func TestDescriptorIndexesIncludesEverySegment(t *testing.T) {
	indexes, err := descriptorIndexes(`{"schema":"public","name":"t","index_name":"current","snapshot_segments":[{"index":"retained1"},{"index":"retained2"}]}`, "public", "t")
	if err != nil || !slices.Equal(indexes, []string{"current", "retained1", "retained2"}) {
		t.Fatalf("indexes=%v err=%v", indexes, err)
	}
	for _, raw := range []string{`{}`, `{"schema":"other","name":"t","index_name":"x"}`, `{"schema":"public","name":"t","index_name":"x","snapshot_segments":[{}]}`} {
		if _, err := descriptorIndexes(raw, "public", "t"); err == nil {
			t.Fatalf("broken descriptor accepted: %s", raw)
		}
	}
}
