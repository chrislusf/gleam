package util

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

var (
	client       *http.Client
	Transport    *http.Transport
	SchemePrefix = "http://"
)

func init() {
	Transport = &http.Transport{
		MaxIdleConnsPerHost: 1024,
	}
	client = &http.Client{Transport: Transport}
}

func Post(url string, values url.Values) ([]byte, error) {
	r, err := client.PostForm(url, values)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func Get(url string) ([]byte, error) {
	r, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if r.StatusCode < 200 || r.StatusCode >= 300 {
		return nil, fmt.Errorf("%s: %s", url, r.Status)
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}

func DownloadUrl(fileUrl string) (filename string, content []byte, e error) {
	response, err := client.Get(fileUrl)
	if err != nil {
		return "", nil, err
	}
	defer response.Body.Close()
	contentDisposition := response.Header["Content-Disposition"]
	if len(contentDisposition) > 0 {
		if strings.HasPrefix(contentDisposition[0], "filename=") {
			filename = contentDisposition[0][len("filename="):]
			filename = strings.Trim(filename, "\"")
		}
	}
	content, e = ioutil.ReadAll(response.Body)
	return
}
