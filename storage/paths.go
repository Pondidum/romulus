package storage

import (
	"fmt"
	"path"
	"time"
)

func spanContentPath(dataset, spanid string) string {
	return path.Join(dataset, "spans", spanid)
}

func tracePath(dataset string, traceid string, spanid string) string {
	return path.Join(dataset, "traces", traceid, spanid)
}

func timesPath(dataset string, t time.Time, spanid string) string {
	epoch := fmt.Sprint(t.Unix())
	return path.Join(dataset, "times", epoch, spanid)
}

func timesPrefixPath(dataset, timePrefix string) string {
	return path.Join(dataset, "times", timePrefix)
}

func attributePath(dataset, attrKey, spanid string) string {
	return path.Join(dataset, "attributes", attrKey, spanid)
}
