package healthz

import (
  "encoding/json"
  "log"
  "net/http"
)

type Config struct {
  Hostname string
}

type KafkaConfig struct {
  DriverName string
  DataSourceName string
}

type handler struct {
  dc *KafkaChecker
  hostname string
  metadata map[string]string
}

func Handler(hc *Config) (http.Handler, error) {
  dc, err := NewKafkaChecker
  if err != nil {
    return nil, err
  }

  config, err :=

  if err != nil {
    return nil, err
  }

  metadata := make(map[string]string)
  metadata["database_url"] = config.Addr
  metadata["database_user"] = config.User

  h := &handler{dc, hc.Hostname, metadata}
  return h, nil
}
