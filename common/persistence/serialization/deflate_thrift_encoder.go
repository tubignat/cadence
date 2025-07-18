// The MIT License (MIT)
//
// Copyright (c) 2017-2020 Uber Technologies Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package serialization

import (
	"bytes"
	"compress/flate"

	"go.uber.org/thriftrw/protocol/binary"

	"github.com/uber/cadence/common/constants"
)

type deflateThriftEncoder struct{}

func newDeflateThriftEncoder() encoder {
	return &deflateThriftEncoder{}
}

func (e *deflateThriftEncoder) shardInfoToBlob(info *ShardInfo) ([]byte, error) {
	return deflateThriftRWEncode(shardInfoToThrift(info))
}

func (e *deflateThriftEncoder) domainInfoToBlob(info *DomainInfo) ([]byte, error) {
	return deflateThriftRWEncode(domainInfoToThrift(info))
}

func (e *deflateThriftEncoder) historyTreeInfoToBlob(info *HistoryTreeInfo) ([]byte, error) {
	return deflateThriftRWEncode(historyTreeInfoToThrift(info))
}

func (e *deflateThriftEncoder) workflowExecutionInfoToBlob(info *WorkflowExecutionInfo) ([]byte, error) {
	return deflateThriftRWEncode(workflowExecutionInfoToThrift(info))
}

func (e *deflateThriftEncoder) activityInfoToBlob(info *ActivityInfo) ([]byte, error) {
	return deflateThriftRWEncode(activityInfoToThrift(info))
}

func (e *deflateThriftEncoder) childExecutionInfoToBlob(info *ChildExecutionInfo) ([]byte, error) {
	return deflateThriftRWEncode(childExecutionInfoToThrift(info))
}

func (e *deflateThriftEncoder) signalInfoToBlob(info *SignalInfo) ([]byte, error) {
	return deflateThriftRWEncode(signalInfoToThrift(info))
}

func (e *deflateThriftEncoder) requestCancelInfoToBlob(info *RequestCancelInfo) ([]byte, error) {
	return deflateThriftRWEncode(requestCancelInfoToThrift(info))
}

func (e *deflateThriftEncoder) timerInfoToBlob(info *TimerInfo) ([]byte, error) {
	return deflateThriftRWEncode(timerInfoToThrift(info))
}

func (e *deflateThriftEncoder) taskInfoToBlob(info *TaskInfo) ([]byte, error) {
	return deflateThriftRWEncode(taskInfoToThrift(info))
}

func (e *deflateThriftEncoder) taskListInfoToBlob(info *TaskListInfo) ([]byte, error) {
	return deflateThriftRWEncode(taskListInfoToThrift(info))
}

func (e *deflateThriftEncoder) transferTaskInfoToBlob(info *TransferTaskInfo) ([]byte, error) {
	return deflateThriftRWEncode(transferTaskInfoToThrift(info))
}

func (e *deflateThriftEncoder) crossClusterTaskInfoToBlob(info *CrossClusterTaskInfo) ([]byte, error) {
	return deflateThriftRWEncode(crossClusterTaskInfoToThrift(info))
}

func (e *deflateThriftEncoder) timerTaskInfoToBlob(info *TimerTaskInfo) ([]byte, error) {
	return deflateThriftRWEncode(timerTaskInfoToThrift(info))
}

func (e *deflateThriftEncoder) replicationTaskInfoToBlob(info *ReplicationTaskInfo) ([]byte, error) {
	return deflateThriftRWEncode(replicationTaskInfoToThrift(info))
}

func (e *deflateThriftEncoder) encodingType() constants.EncodingType {
	return constants.EncodingTypeThriftRWDeflate
}

func deflateThriftRWEncode(t thriftRWType) ([]byte, error) {
	var b bytes.Buffer
	sw := binary.Default.Writer(&b)
	defer sw.Close()
	if err := t.Encode(sw); err != nil {
		return nil, err
	}

	var compressed bytes.Buffer
	w, err := flate.NewWriter(&compressed, flate.DefaultCompression)
	if err != nil {
		return nil, err
	}
	defer w.Close()

	if _, err := w.Write(b.Bytes()); err != nil {
		return nil, err
	}

	if err := w.Close(); err != nil {
		return nil, err
	}

	return compressed.Bytes(), nil
}
