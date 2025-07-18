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
	"io"

	"go.uber.org/thriftrw/protocol/binary"

	"github.com/uber/cadence/.gen/go/sqlblobs"
	"github.com/uber/cadence/common/metrics"
)

type (
	deflateThriftDecoder struct {
		scope metrics.Scope
	}
)

func newDeflateThriftDecoder() decoder {
	return &deflateThriftDecoder{
		scope: metrics.NoopScope,
	}
}

func (d *deflateThriftDecoder) shardInfoFromBlob(data []byte) (*ShardInfo, error) {
	result := &sqlblobs.ShardInfo{}
	if err := deflateThriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return shardInfoFromThrift(result), nil
}

func (d *deflateThriftDecoder) domainInfoFromBlob(data []byte) (*DomainInfo, error) {
	result := &sqlblobs.DomainInfo{}
	if err := deflateThriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return domainInfoFromThrift(result), nil
}

func (d *deflateThriftDecoder) historyTreeInfoFromBlob(data []byte) (*HistoryTreeInfo, error) {
	result := &sqlblobs.HistoryTreeInfo{}
	if err := deflateThriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return historyTreeInfoFromThrift(result), nil
}

func (d *deflateThriftDecoder) workflowExecutionInfoFromBlob(data []byte) (*WorkflowExecutionInfo, error) {
	result := &sqlblobs.WorkflowExecutionInfo{}
	if err := deflateThriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return workflowExecutionInfoFromThrift(result), nil
}

func (d *deflateThriftDecoder) activityInfoFromBlob(data []byte) (*ActivityInfo, error) {
	result := &sqlblobs.ActivityInfo{}
	if err := deflateThriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return activityInfoFromThrift(result), nil
}

func (d *deflateThriftDecoder) childExecutionInfoFromBlob(data []byte) (*ChildExecutionInfo, error) {
	result := &sqlblobs.ChildExecutionInfo{}
	if err := deflateThriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return childExecutionInfoFromThrift(result), nil
}

func (d *deflateThriftDecoder) signalInfoFromBlob(data []byte) (*SignalInfo, error) {
	result := &sqlblobs.SignalInfo{}
	if err := deflateThriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return signalInfoFromThrift(result), nil
}

func (d *deflateThriftDecoder) requestCancelInfoFromBlob(data []byte) (*RequestCancelInfo, error) {
	result := &sqlblobs.RequestCancelInfo{}
	if err := deflateThriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return requestCancelInfoFromThrift(result), nil
}

func (d *deflateThriftDecoder) timerInfoFromBlob(data []byte) (*TimerInfo, error) {
	result := &sqlblobs.TimerInfo{}
	if err := deflateThriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return timerInfoFromThrift(result), nil
}

func (d *deflateThriftDecoder) taskInfoFromBlob(data []byte) (*TaskInfo, error) {
	result := &sqlblobs.TaskInfo{}
	if err := deflateThriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return taskInfoFromThrift(result), nil
}

func (d *deflateThriftDecoder) taskListInfoFromBlob(data []byte) (*TaskListInfo, error) {
	result := &sqlblobs.TaskListInfo{}
	if err := deflateThriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return taskListInfoFromThrift(result), nil
}

func (d *deflateThriftDecoder) transferTaskInfoFromBlob(data []byte) (*TransferTaskInfo, error) {
	result := &sqlblobs.TransferTaskInfo{}
	if err := deflateThriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return transferTaskInfoFromThrift(result), nil
}

func (d *deflateThriftDecoder) crossClusterTaskInfoFromBlob(data []byte) (*CrossClusterTaskInfo, error) {
	result := &sqlblobs.TransferTaskInfo{}
	if err := deflateThriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return crossClusterTaskInfoFromThrift(result), nil
}

func (d *deflateThriftDecoder) timerTaskInfoFromBlob(data []byte) (*TimerTaskInfo, error) {
	result := &sqlblobs.TimerTaskInfo{}
	if err := deflateThriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return timerTaskInfoFromThrift(result), nil
}

func (d *deflateThriftDecoder) replicationTaskInfoFromBlob(data []byte) (*ReplicationTaskInfo, error) {
	result := &sqlblobs.ReplicationTaskInfo{}
	if err := deflateThriftRWDecode(data, result); err != nil {
		return nil, err
	}
	return replicationTaskInfoFromThrift(result), nil
}

func deflateThriftRWDecode(b []byte, result thriftRWType) error {
	reader := flate.NewReader(bytes.NewReader(b))
	defer reader.Close()

	var decompressed bytes.Buffer
	if _, err := io.Copy(&decompressed, reader); err != nil {
		return err
	}

	buf := bytes.NewReader(decompressed.Bytes())
	sr := binary.Default.Reader(buf)
	return result.Decode(sr)
}
