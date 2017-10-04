package main

import (
	"flag"
	"github.com/minio/minio-go"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"time"
    "strconv"

	"cloud.google.com/go/storage"
	"encoding/hex"
	"github.com/goinggo/workpool"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

type MyWork struct {
	s3fileName   string
	GCbucket     string
	s3bucketname string
	filename     string
	GCClient     *storage.Client
	ctx          context.Context
	s3Client     *minio.Client
	WP           *workpool.WorkPool
	count        *int
}

func (mw *MyWork) DoWork(workRoutine int) {
	//open s3 file
	s3reader, err := mw.s3Client.GetObject(mw.s3bucketname, mw.s3fileName, minio.GetObjectOptions{})
	if err != nil {
		log.Print(mw.s3fileName, err)
	}
	defer s3reader.Close()
	//open GC bucket
	bh := mw.GCClient.Bucket(mw.GCbucket)

	//GC file to open
	obj := bh.Object(mw.s3bucketname + "/" + mw.s3fileName)
	GCw := obj.NewWriter(mw.ctx)
	defer GCw.Close()

	w, err := os.Create(mw.filename)
	if err != nil {
		log.Fatalln(err)
	}
	defer w.Close()

	//s3 file info
	stat, err := s3reader.Stat()
	if err != nil {
		log.Print(err)
		os.Remove(mw.filename)
		return
	}

	if _, err := io.CopyN(w, s3reader, stat.Size); err != nil {
		log.Print(mw.s3fileName, err)
		os.Remove(mw.filename)
		return
	}

	r, err := os.Open(mw.filename)
	if err != nil {
		log.Print(err)
		os.Remove(mw.filename)
		return
	}
	defer r.Close()

	if _, err := io.CopyN(GCw, r, stat.Size); err != nil {
		log.Print(mw.s3fileName, err)
		os.Remove(mw.filename)
		return
	}
	{
        *mw.count++
		log.Print(mw.s3bucketname+"/"+mw.s3fileName, " End GC Copy, file num " + strconv.Itoa(*mw.count) + "\n")
	}

	err = os.Remove(mw.filename)
	if err != nil {
		log.Print(err)
		return
	}
}

func getGSfileMap(client *storage.Client, ctx context.Context, bucket, prefix, delim string) (map[string]string, error) {
	it := client.Bucket(bucket).Objects(ctx, &storage.Query{
		Prefix:    prefix,
		Delimiter: delim,
	})
	fileMap := make(map[string]string)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Println(err)
			return nil, err
		}
		if attrs.Name[len(attrs.Name)-1:] != "/" {
			fileMap[attrs.Name] = hex.EncodeToString(attrs.MD5)
		}
	}
	return fileMap, nil
}

func main() {   
    s3endpoint := flag.String("s3endpoint", "", "s3 url")
    s3accessKeyID := flag.String("s3ID", "", "s3 access ID")
    s3secretAccessKey := flag.String("s3Key", "", "s3 access Key")
    s3useSSL := flag.Bool("s3ssl", false, "use ssl for s3")
    projectID := flag.String("GCproject", "", "gcloud projectID")
    GSbucketName := flag.String("GCbucketName", "", "gcloud bucket name")

    flag.Parse()
    
	if *projectID == "" {
		log.Fatalf("GCproject variable must be set, use --GCproject .\n")
	}    
    
	var queueCapacity int32 = 5000
	count := 0

	runtime.GOMAXPROCS(runtime.NumCPU())

	workPool := workpool.New(runtime.NumCPU(), queueCapacity)

	ctx := context.Background()

	// Creates a GCClient.
	GCClient, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Initialize minio client object.
	s3Client, err := minio.New(*s3endpoint, *s3accessKeyID, *s3secretAccessKey, *s3useSSL)
	if err != nil {
		log.Fatalln(err)
	}

	s3buckets, err := s3Client.ListBuckets()
	if err != nil {
		log.Fatalln(err)
	}
	for _, s3bucket := range s3buckets {
		if s3bucket.Name == "ridero-cache" {
			continue
		}
		GSFileMap := make(map[string]string)
        log.Println("Create GS file map")
		GSFileMap, err = getGSfileMap(GCClient, ctx, *GSbucketName, s3bucket.Name+"/", "")
		if err != nil {
			log.Fatal(err)
		}

		doneCh := make(chan struct{})
		defer close(doneCh)
        log.Println("Compare GS file map to s3 files")
		for s3file := range s3Client.ListObjects(s3bucket.Name, "", true, doneCh) {
			if s3file.Err != nil {
				log.Println(s3file.Err)
			}
			if "\""+GSFileMap[s3bucket.Name+"/"+s3file.Key]+"\"" != s3file.ETag {
				work := MyWork{
					s3fileName:   s3file.Key,
					GCbucket:     *GSbucketName,
					s3bucketname: s3bucket.Name,
					ctx:          ctx,
					s3Client:     s3Client,
					GCClient:     GCClient,
					WP:           workPool,
					filename:     "./tmp/" + strings.Split(s3file.ETag, "\"")[1],
					count:        &count,
				}

				for queueCapacity-10 < workPool.QueuedWork() {
					time.Sleep(100 * time.Millisecond)
				}

				if err := workPool.PostWork("routine", &work); err != nil {
					log.Printf("ERROR: %s\n", err)
				}
			}
		}
	}

}
