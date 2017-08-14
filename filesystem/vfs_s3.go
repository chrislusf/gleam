package filesystem

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	AWS_ACCESS_KEY = OptionName("aws_access_key")
	AWS_SECRET_KEY = OptionName("aws_secret_key")
)

type S3FileSystem struct {
}

func (fs *S3FileSystem) Accept(fl *FileLocation) bool {
	return strings.HasPrefix(fl.Location, "s3://")
}

func (fs *S3FileSystem) Open(fl *FileLocation) (VirtualFile, error) {
	sess, err := session.NewSession(aws.NewConfig().WithCredentials(
		credentials.NewStaticCredentials(Option[AWS_ACCESS_KEY], Option[AWS_SECRET_KEY], ""),
	))
	if err != nil {
		fmt.Println("failed to create session,", err)
		return nil, err
	}

	svc := s3.New(sess)

	bucketName, objectKey, err := splitS3LocationToParts(fl.Location)

	if err != nil {
		return nil, fmt.Errorf("Failed to split S3 location to parts %s: %v", fl.Location, err)
	}

	params := &s3.GetObjectInput{
		Bucket: aws.String(bucketName), // Required
		Key:    aws.String(objectKey),  // Required
	}
	resp, err := svc.GetObject(params)

	if err != nil {
		return nil, err
	}

	return newVirtualFileS3(resp.Body)
}

func (fs *S3FileSystem) List(fl *FileLocation) (fileLocations []*FileLocation, err error) {
	return nil, fmt.Errorf("S3 Listing is not supported yet.")
}

func (fs *S3FileSystem) IsDir(fl *FileLocation) bool {
	return false
}

func splitS3LocationToParts(location string) (bucketName, objectKey string, err error) {
	s3Prefix := "s3://"
	if !strings.HasPrefix(location, s3Prefix) {
		return "", "", fmt.Errorf("parameter %s should start with hdfs://", location)
	}

	parts := strings.SplitN(location[len(s3Prefix):], "/", 2)
	return parts[0], parts[1], nil
}

type VirtualFileS3 struct {
	*os.File
	filename string
	size     int64
}

func newVirtualFileS3(readerCloser io.ReadCloser) (*VirtualFileS3, error) {
	filename := fmt.Sprintf("%s/s3_%d", os.TempDir(), rand.Uint32())
	outFile, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	size, err := io.Copy(outFile, readerCloser)
	readerCloser.Close()

	outFile.Seek(0, 0)

	return &VirtualFileS3{outFile, filename, size}, err

}

func (vf *VirtualFileS3) Size() int64 {
	return vf.size
}

func (vf *VirtualFileS3) Close() error {
	vf.File.Close()
	return os.Remove(vf.filename)
}
