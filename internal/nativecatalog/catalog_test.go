package nativecatalog

import (
	"slices"
	"testing"
)

func TestDescriptorIndexesIncludesEverySegment(t *testing.T) {
	descriptor, err := ParseTable(`{"schema":"public","name":"t","index_name":"current","snapshot_segments":[{"index":"retained1"},{"index":"retained2"}]}`, "public", "t")
	indexes := descriptor.Indexes()
	if err != nil || !slices.Equal(indexes, []string{"current", "retained1", "retained2"}) {
		t.Fatalf("indexes=%v err=%v", indexes, err)
	}
	for _, raw := range []string{`{}`, `{"schema":"other","name":"t","index_name":"x"}`, `{"schema":"public","name":"t","index_name":"x","snapshot_segments":[{}]}`} {
		if _, err := ParseTable(raw, "public", "t"); err == nil {
			t.Fatalf("broken descriptor accepted: %s", raw)
		}
	}
}

func TestMetadataKeyEscapingPreservesDistinctIdentifiers(t *testing.T) {
	for input, want := range map[string]string{"a:b": "a%3Ab", "a{meta}": "a%7Bmeta%7D", "a%20": "a%2520", "a b": "a%20b", "雪": "雪"} {
		if got := KeyPart(input); got != want {
			t.Errorf("KeyPart(%q)=%q, want %q", input, got, want)
		}
	}
}
