package ossstore

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/tus/tusd/v2/internal/semaphore"
	"regexp"
)

// This regular expression matches every character which is not
// considered valid into a header value according to RFC2616.
var nonPrintableRegexp = regexp.MustCompile(`[^\x09\x20-\x7E]`)

// See the handler.DataStore interface for documentation about the different
// methods.
type OSSStore struct {
	// Bucket used to store the data in, e.g. "tusdstore.example.com"
	Bucket string
	// ObjectPrefix is prepended to the name of each S3 object that is created
	// to store uploaded files. It can be used to create a pseudo-directory
	// structure in the bucket, e.g. "path/to/my/uploads".
	ObjectPrefix string
	// MetadataObjectPrefix is prepended to the name of each .info and .part S3
	// object that is created. If it is not set, then ObjectPrefix is used.
	MetadataObjectPrefix string
	// Service specifies an interface used to communicate with the S3 backend.
	// Usually, this is an instance of github.com/aws/aws-sdk-go-v2/service/s3.Client
	// (https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/s3#Client).
	Service OSSAPI
	// MaxPartSize specifies the maximum size of a single part uploaded to S3
	// in bytes. This value must be bigger than MinPartSize! In order to
	// choose the correct number, two things have to be kept in mind:
	//
	// If this value is too big and uploading the part to S3 is interrupted
	// expectedly, the entire part is discarded and the end user is required
	// to resume the upload and re-upload the entire big part. In addition, the
	// entire part must be written to disk before submitting to S3.
	//
	// If this value is too low, a lot of requests to S3 may be made, depending
	// on how fast data is coming in. This may result in an eventual overhead.
	MaxPartSize int64
	// MinPartSize specifies the minimum size of a single part uploaded to S3
	// in bytes. This number needs to match with the underlying S3 backend or else
	// uploaded parts will be reject. AWS S3, for example, uses 5MB for this value.
	MinPartSize int64
	// PreferredPartSize specifies the preferred size of a single part uploaded to
	// S3. S3Store will attempt to slice the incoming data into parts with this
	// size whenever possible. In some cases, smaller parts are necessary, so
	// not every part may reach this value. The PreferredPartSize must be inside the
	// range of MinPartSize to MaxPartSize.
	PreferredPartSize int64
	// MaxMultipartParts is the maximum number of parts an S3 multipart upload is
	// allowed to have according to AWS S3 API specifications.
	// See: http://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
	MaxMultipartParts int64
	// MaxObjectSize is the maximum size an S3 Object can have according to S3
	// API specifications. See link above.
	MaxObjectSize int64
	// MaxBufferedParts is the number of additional parts that can be received from
	// the client and stored on disk while a part is being uploaded to S3. This
	// can help improve throughput by not blocking the client while tusd is
	// communicating with the S3 API, which can have unpredictable latency.
	MaxBufferedParts int64
	// TemporaryDirectory is the path where S3Store will create temporary files
	// on disk during the upload. An empty string ("", the default value) will
	// cause S3Store to use the operating system's default temporary directory.
	TemporaryDirectory string
	// DisableContentHashes instructs the S3Store to not calculate the MD5 and SHA256
	// hashes when uploading data to S3. These hashes are used for file integrity checks
	// and for authentication. However, these hashes also consume a significant amount of
	// CPU, so it might be desirable to disable them.
	// Note that this property is experimental and might be removed in the future!
	DisableContentHashes bool

	// uploadSemaphore limits the number of concurrent multipart part uploads to S3.
	uploadSemaphore semaphore.Semaphore

	// requestDurationMetric holds the prometheus instance for storing the request durations.
	requestDurationMetric *prometheus.SummaryVec

	// diskWriteDurationMetric holds the prometheus instance for storing the time it takes to write chunks to disk.
	diskWriteDurationMetric prometheus.Summary

	// uploadSemaphoreDemandMetric holds the prometheus instance for storing the demand on the upload semaphore
	uploadSemaphoreDemandMetric prometheus.Gauge

	// uploadSemaphoreLimitMetric holds the prometheus instance for storing the limit on the upload semaphore
	uploadSemaphoreLimitMetric prometheus.Gauge
}

// The labels to use for observing and storing request duration. One label per operation.
const (
	metricGetInfoObject           = "get_info_object"
	metricPutInfoObject           = "put_info_object"
	metricCreateMultipartUpload   = "create_multipart_upload"
	metricCompleteMultipartUpload = "complete_multipart_upload"
	metricUploadPart              = "upload_part"
	metricListParts               = "list_parts"
	metricHeadPartObject          = "head_part_object"
	metricGetPartObject           = "get_part_object"
	metricPutPartObject           = "put_part_object"
	metricDeletePartObject        = "delete_part_object"
)

// https://github.com/aliyun/aliyun-oss-go-sdk/tree/master
type OSSAPI interface {
	PutObject(ctx context.Context, input *s3.PutObjectInput, opt ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	//ListParts(ctx context.Context, input *s3.ListPartsInput, opt ...func(*s3.Options)) (*s3.ListPartsOutput, error)
	UploadPart(ctx context.Context, input *s3.UploadPartInput, opt ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	GetObject(ctx context.Context, input *s3.GetObjectInput, opt ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	//GetObjectDetailedMeta ?
	HeadObject(ctx context.Context, input *s3.HeadObjectInput, opt ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	CreateMultipartUpload(ctx context.Context, input *s3.CreateMultipartUploadInput, opt ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	AbortMultipartUpload(ctx context.Context, input *s3.AbortMultipartUploadInput, opt ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	DeleteObject(ctx context.Context, input *s3.DeleteObjectInput, opt ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	DeleteObjects(ctx context.Context, input *s3.DeleteObjectsInput, opt ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput, opt ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	UploadPartCopy(ctx context.Context, input *s3.UploadPartCopyInput, opt ...func(*s3.Options)) (*s3.UploadPartCopyOutput, error)
}

// New constructs a new storage using the supplied bucket and service object.
func New(bucket string, service OSSAPI) OSSStore {
	requestDurationMetric := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "tusd_aliyunoss_request_duration_ms",
		Help:       "Duration of requests sent to AliyunOSS in milliseconds per operation",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	}, []string{"operation"})

	diskWriteDurationMetric := prometheus.NewSummary(prometheus.SummaryOpts{
		Name:       "tusd_aliyunoss_disk_write_duration_ms",
		Help:       "Duration of chunk writes to disk in milliseconds",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	})

	uploadSemaphoreDemandMetric := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "tusd_aliyunoss_upload_semaphore_demand",
		Help: "Number of goroutines wanting to acquire the upload lock or having it acquired",
	})

	uploadSemaphoreLimitMetric := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "tusd_aliyunoss_upload_semaphore_limit",
		Help: "Limit of concurrent acquisitions of upload semaphore",
	})

	store := OSSStore{
		Bucket:                      bucket,
		Service:                     service,
		MaxPartSize:                 5 * 1024 * 1024 * 1024,
		MinPartSize:                 5 * 1024 * 1024,
		PreferredPartSize:           50 * 1024 * 1024,
		MaxMultipartParts:           10000,
		MaxObjectSize:               5 * 1024 * 1024 * 1024 * 1024,
		MaxBufferedParts:            20,
		TemporaryDirectory:          "",
		requestDurationMetric:       requestDurationMetric,
		diskWriteDurationMetric:     diskWriteDurationMetric,
		uploadSemaphoreDemandMetric: uploadSemaphoreDemandMetric,
		uploadSemaphoreLimitMetric:  uploadSemaphoreLimitMetric,
	}

	store.SetConcurrentPartUploads(10)
	return store
}

// SetConcurrentPartUploads changes the limit on how many concurrent part uploads to AliyunOSS are allowed.
func (store *OSSStore) SetConcurrentPartUploads(limit int) {
	store.uploadSemaphore = semaphore.New(limit)
	store.uploadSemaphoreLimitMetric.Set(float64(limit))
}
