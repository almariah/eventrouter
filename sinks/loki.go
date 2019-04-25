/*
Copyright 2017 Heptio Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sinks

import (
	"time"
	loki "github.com/grafana/loki/pkg/promtail/client"
	"github.com/golang/glog"
	"k8s.io/api/core/v1"

	"fmt"
	"github.com/go-kit/kit/log"
	"os"

	"bytes"
	"encoding/json"
)

/*
The Loki sink is a sink that sends events over HTTP using protobuf.
It uses promtail client with the remote loki endpoint, sending messages
as batches async.
*/

// LokiSink wraps an Loki client that messages should be sent to.
type LokiSink struct {
	lokiClient *loki.Client
	lokiExcludedLabels []string
}

// NewLokiSink constructs a new LokiSink given a sink URL, backoff setting and excludedLabels
func NewLokiSink(url string, batchWait int, batchSize int, minBackoff int, maxBackoff int, maxRetries int, timeout int, excludedLabels []string) (EventSinkInterface, error) {

	lokiConfig := loki.Config{
		BatchWait: time.Millisecond * time.Duration(batchWait),
		BatchSize: batchSize,
		Timeout: time.Millisecond * time.Duration(timeout),
	}

	lokiConfig.SetURL(url)
	lokiConfig.SetMinBackoff(maxBackoff)
	lokiConfig.SetMaxBackoff(minBackoff)
	lokiConfig.SetMaxRetriesBackoff(maxRetries)

	w := log.NewSyncWriter(os.Stderr)
	logger := log.NewLogfmtLogger(w)

	client, err := loki.New(lokiConfig, logger)

	return &LokiSink{
		lokiClient: client,
		lokiExcludedLabels: excludedLabels,
	}, err

}

// UpdateEvents implements the EventSinkInterface.
func (l *LokiSink) UpdateEvents(eNew *v1.Event, eOld *v1.Event) {

	// eOld could be nil
	eData := NewEventData(eNew, eOld)
	eData.OldEvent = nil

	message := BuildEventMessage(eData.Event)

	labels := LokiLabelSet(eData)

	for _, label := range l.lokiExcludedLabels {
		delete(labels, label)
	}

	err := l.lokiClient.Handle(loki.BuildLabelSet(labels), eData.Event.CreationTimestamp.Time, message)
	if err != nil {
		glog.Errorf("Failed to produce message: %v", err)
	}

}

// LokiLabelSet generates loki labels
func LokiLabelSet(event EventData) map[string]string {

	buf := bytes.NewBuffer(make([]byte, 0, 4096))
	written, err := event.WriteFlattenedJSON(buf)
	if err != nil {
		glog.Warningf("Could not write to event request body (wrote %v) bytes: %v", written, err)
		return nil
	}

	labels := make(map[string]interface{})
	err = json.Unmarshal([]byte(buf.String()), &labels)
	if err != nil {
		panic(err)
	}

	labelsString := make(map[string]string)
	for k, v := range labels {
		if str, ok := v.(string); ok {
	    labelsString[k] = str
		} else if n, ok := v.(int); ok {
			labelsString[k] = string(n)
		}
	}

	labelsString["event_verb"] = labelsString["verb"]
	labelsString["namespace"] = labelsString["event_object_meta_namespace"]
	delete(labelsString, "verb")
	delete(labelsString, "event_object_meta_namespace")
	delete(labelsString, "event_message")
	delete(labelsString, "event_object_meta_creation_timestamp")

	return labelsString

}

// BuildEventMessage format the log message that will be sent by Loki
func BuildEventMessage(event *v1.Event) string {

	var level string

	if event.Type == "Normal" {
		level = "info"
	} else {
		level = "error"
	}

	message := fmt.Sprintf("level=%s message=\"%s\"", level, event.Message)

	return message
}
