package util

import "fmt"

// SliceSliceToSliceMap converts [][]any to []map[string]any using field names.
// Returns an error if any row has fewer values than the number of fields.
func SliceSliceToSliceMap(fields []string, data [][]any) ([]map[string]any, error) {
	fieldsLen := len(fields)
	sm := make([]map[string]any, 0, len(data))
	for i, item := range data {
		if len(item) < fieldsLen {
			return nil, fmt.Errorf("data[%d] has %d values, need %d", i, len(item), fieldsLen)
		}
		m := make(map[string]any, fieldsLen)
		for k, field := range fields {
			m[field] = item[k]
		}

		sm = append(sm, m)
	}

	return sm, nil
}
