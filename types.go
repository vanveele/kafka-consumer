package main

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
