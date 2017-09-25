package main

import "regexp"

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

type StructuredData struct {
	ID     string
	Params map[string]string
}

// ------------------------------------------------
// https://tools.ietf.org/html/rfc5424#section-6.3
// ------------------------------------------------

func parseStructuredData(sd string) (map[string]StructuredData, error) {
	sdKV, _ := regexp.Compile(`(\S+)="([^"]+)"`)
	sdSub, _ := regexp.Compile(`\[(\S+)\s+([^\]]+)]`)

	ids := sdSub.FindAllStringSubmatch(sd, -1)

	res := make(map[string]StructuredData)
	for _, id := range ids {
		i := id[1]
		j := id[2]
		data := sdKV.FindAllStringSubmatch(j, -1)
		params := make(map[string]string)
		for _, kv := range data {
			k := kv[1]
			v := kv[2]
			params[k] = v
		}
		res[i] = StructuredData{ID: i, Params: params}
	}

	return res, nil
}
