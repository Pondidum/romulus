package util

func CommonPrefix(a, b string) string {
	pos := 0

	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			break
		}
		pos++
	}

	return a[:pos]
}
