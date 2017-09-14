package main

import (
	"regexp"
)

// DataPoint is a typle of [UNIX timestamp, value]. This has to use floats
// because the value could be non-integer.
type DataPoint [2]float64

type Metric struct {
	Metric string      `json:"metric,omitempty"`
	Points []DataPoint `json:"points,omitempty"`
	Type   string      `json:"type,omitempty"`
	Host   string      `json:"host,omitempty"`
	Tags   []string    `json:"tags,omitempty"`
	Unit   *string     `json:"unit,omitempty"`
}

type Series struct {
	Series []Metric `json:"series,omitempty"`
}

// ------------------------------------------------
// https://tools.ietf.org/html/rfc5424#section-6.3
// ------------------------------------------------

func parseStructuredData(sd *string) map[string]string {
	i, _ := regexp.Compile("(index|sourcetype|source|appid|host)=(\\S+)")
	data := i.FindAllStringSubmatch(*sd, -1)

	res := make(map[string]string)
	for _, kv := range data {
		k := kv[1]
		v := kv[2]
		res[k] = v
	}

	return res
}
