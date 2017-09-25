package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	sd1 = `[myParamId@12345 class="high" style="wide"]`
	sd2 = `[splunk::v1@11223 index="foo" sourcetype="bar" appid="00001111-2222-3333-4444-55556666677777"]`
	sd3 = `[splunk::v1@11223 sourcetype="bar" index="foo" appid="00001111-2222-3333-4444-55556666677777"]`
	sd4 = `[myParamId@12345 class="high" style="wide"][splunk::v1@11223 sourcetype="bar" index="foo" appid="00001111-2222-3333-4444-55556666677777"]`
	sd5 = `[splunk::v1@11223 sourcetype="bar" index="foo" appid="00001111-2222-3333-4444-55556666677777"][myParamId@12345 class="high" style="wide"]`
)

func TestParseStructuredData(t *testing.T) {
	id := "splunk::v1@11223"
	res, err := parseStructuredData(sd1)
	assert.Nil(t, res[id])

	res, err = parseStructuredData(sd2)
	assert.Nil(t, err)
	fmt.Printf("%v\n%v\n", sd2, res[id])

	res, err = parseStructuredData(sd3)
	assert.Nil(t, err)
	fmt.Printf("%v\n%v\n", sd3, res[id])

	res, err = parseStructuredData(sd4)
	assert.Nil(t, err)
	fmt.Printf("%v\n%v\n", sd4, res[id])

	res, err = parseStructuredData(sd5)
	assert.Nil(t, err)
	fmt.Printf("%v\n%v\n", sd5, res[id])
}
