package hdfs

import (
	"encoding/xml"
	"errors"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// Property is the struct representation of hadoop configuration
// key value pair.
type Property struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
}

type propertyList struct {
	Property []Property `xml:"property"`
}

// HadoopConf represents a map of all the key value configutation
// pairs found in a user's hadoop configuration files.
type HadoopConf map[string]string

var errUnresolvedNamenode = errors.New("no namenode address in configuration")

// LoadHadoopConf returns a HadoopConf object that is key value map
// of all the hadoop conf properties, swallows errors reading xml
// and reading a non-existant file.
func LoadHadoopConf(inputPath string) HadoopConf {
	var tryPaths []string

	if inputPath != "" {
		tryPaths = append(tryPaths, inputPath)
	} else {
		hadoopConfDir := os.Getenv("HADOOP_CONF_DIR")
		hadoopHome := os.Getenv("HADOOP_HOME")
		if hadoopConfDir != "" {
			confHdfsPath := filepath.Join(hadoopConfDir, "hdfs-site.xml")
			confCorePath := filepath.Join(hadoopConfDir, "core-site.xml")
			tryPaths = append(tryPaths, confHdfsPath, confCorePath)
		}
		if hadoopHome != "" {
			hdfsPath := filepath.Join(hadoopHome, "conf", "hdfs-site.xml")
			corePath := filepath.Join(hadoopHome, "conf", "core-site.xml")
			tryPaths = append(tryPaths, hdfsPath, corePath)
		}
	}
	hadoopConf := make(HadoopConf)

	for _, tryPath := range tryPaths {
		pList := propertyList{}
		f, err := ioutil.ReadFile(tryPath)
		if err != nil {
			continue
		}

		xmlErr := xml.Unmarshal(f, &pList)
		if xmlErr != nil {
			continue
		}

		for _, prop := range pList.Property {
			hadoopConf[prop.Name] = prop.Value
		}
	}
	return hadoopConf
}

// Namenodes returns a slice of deduplicated namenodes named in
// a user's hadoop configuration files or an error is there are no namenodes.
func (conf HadoopConf) Namenodes() ([]string, error) {
	nns := make(map[string]bool)
	for key, value := range conf {
		if strings.Contains(key, "fs.defaultFS") {
			nnUrl, _ := url.Parse(value)
			nns[nnUrl.Host] = true
		}
		if strings.HasPrefix(key, "dfs.namenode.rpc-address") {
			nns[value] = true
		}
	}
	if len(nns) == 0 {
		return nil, errUnresolvedNamenode
	}

	keys := make([]string, len(nns))

	i := 0
	for k, _ := range nns {
		keys[i] = k
		i++
	}
	return keys, nil
}
