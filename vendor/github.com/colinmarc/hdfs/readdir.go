package hdfs

import (
	"os"
	"path"

	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/rpc"
	"github.com/golang/protobuf/proto"
)

// ReadDir reads the directory named by dirname and returns a list of sorted
// directory entries.
func (c *Client) ReadDir(dirname string) ([]os.FileInfo, error) {
	return c.getDirList(dirname, "", 0)
}

func (c *Client) getDirList(dirname string, after string, max int) ([]os.FileInfo, error) {
	var res []os.FileInfo
	last := after
	for max <= 0 || len(res) < max {
		partial, remaining, err := c.getPartialDirList(dirname, last)
		if err != nil {
			return nil, err
		}

		res = append(res, partial...)
		if remaining == 0 {
			break
		} else if len(partial) > 0 {
			last = partial[len(partial)-1].Name()
		}
	}

	if max > 0 && len(res) > max {
		res = res[:max]
	}

	return res, nil
}

func (c *Client) getPartialDirList(dirname string, after string) ([]os.FileInfo, int, error) {
	dirname = path.Clean(dirname)

	req := &hdfs.GetListingRequestProto{
		Src:          proto.String(dirname),
		StartAfter:   []byte(after),
		NeedLocation: proto.Bool(true),
	}
	resp := &hdfs.GetListingResponseProto{}

	err := c.namenode.Execute("getListing", req, resp)
	if err != nil {
		if nnErr, ok := err.(*rpc.NamenodeError); ok {
			err = interpretException(nnErr.Exception, err)
		}

		return nil, 0, &os.PathError{"readdir", dirname, err}
	} else if resp.GetDirList() == nil {
		return nil, 0, &os.PathError{"readdir", dirname, os.ErrNotExist}
	}

	list := resp.GetDirList().GetPartialListing()
	res := make([]os.FileInfo, 0, len(list))
	for _, status := range list {
		res = append(res, newFileInfo(status, ""))
	}

	remaining := int(resp.GetDirList().GetRemainingEntries())
	return res, remaining, nil
}
