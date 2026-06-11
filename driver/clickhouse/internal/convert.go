package internal

// SliceSliceToSliceMap converts [][]any to []map[string]any using field names.
func SliceSliceToSliceMap(fields []string, data [][]any) []map[string]any {
	fieldsLen := len(fields)
	sm := make([]map[string]any, 0, len(data))
	for _, item := range data {
		m := make(map[string]any, fieldsLen)
		for k, field := range fields {
			m[field] = item[k]
		}

		sm = append(sm, m)
	}

	return sm
}
