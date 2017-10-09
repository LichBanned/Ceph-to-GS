package main

import (
	"flag"
	"io"
	"log"
	"runtime"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"encoding/hex"
	"github.com/go-redis/redis"
	"github.com/goinggo/workpool"
	"github.com/minio/minio-go"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

type MyWork struct {
	s3Client     *minio.Client
	s3fileName   string
	s3bucketname string
	GSbucket     string
	GSClient     *storage.Client
	count        *int
}

func main() {
	s3endpoint := flag.String("s3ep", "", "s3 url")
	s3accessKeyID := flag.String("s3id", "", "s3 access ID")
	s3secretAccessKey := flag.String("s3key", "", "s3 access Key")
	s3useSSL := flag.Bool("s3ssl", false, "use ssl for s3")
	projectID := flag.String("gsproject", "", "gcloud projectID")
	GSbucketName := flag.String("gsbucket", "", "gcloud bucket name")
	excludeBucket := flag.String("exclude", "", "comma separated s3 bucket names to exclude from process")
	copyBucket := flag.String("copy", "", "comma separated s3 bucket names to process, do not read bucket list from s3")
	getNewFiles := flag.Bool("getnew", true, "Get files modified in 24h")
	redisHost := flag.String("rhost", "localhost:6379", "redis server address")
	redisPass := flag.String("rpass", "", "redis password")
	redisDB := flag.Int("rdb", 0, "redis database")

	flag.Parse()

	if *projectID == "" || *s3endpoint == "" || *GSbucketName == "" {
		log.Fatalf("GSproject, GSbucketName, s3endpoint variables must be set.\n")
	}

	if *excludeBucket != "" && *copyBucket != "" {
		log.Fatalf("Not use excludeBucket and copyBucket at same time ")
	}

	var queueCapacity int32 = 1000
	count := 0

	runtime.GOMAXPROCS(runtime.NumCPU())

	workPool := workpool.New(runtime.NumCPU(), queueCapacity)

	// Creates a redis Сlient
	redisСlient := redis.NewClient(&redis.Options{
		Addr:     *redisHost,
		Password: *redisPass,
		DB:       *redisDB,
	})
	if _, err := redisСlient.Ping().Result(); err != nil {
		log.Fatalln(err)
	}
	defer redisСlient.Close()

	// Creates a GS Client.
	ctx := context.Background()
	GSClient, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer GSClient.Close()

	// Initialize minio client object.
	s3Client, err := minio.New(*s3endpoint, *s3accessKeyID, *s3secretAccessKey, *s3useSSL)
	if err != nil {
		log.Fatalln(err)
	}

	s3buckets, err := getS3buckets(s3Client, excludeBucket, copyBucket)
	if err != nil {
		log.Fatalln(err)
	}
	for _, s3bucket := range s3buckets {
		redisСlient.FlushAll()
		log.Println("Create GS file map", s3bucket)
		if err := getGSfileMap(redisСlient, GSClient, ctx, *GSbucketName, s3bucket+"/", ""); err != nil {
			log.Fatal(err)
		}

		doneCh := make(chan struct{})
		defer close(doneCh)
		log.Println("Compare GS file map to s3 files", s3bucket)
		for s3file := range s3Client.ListObjects(s3bucket, "", true, doneCh) {
			if s3file.Err != nil {
				log.Println(s3file.Err)
				continue
			}
			s3md5 := strings.Replace(s3file.ETag, "\"", "", -1)
			GSfilename := getGSfilename(s3bucket, s3file.Key)
			if s3file.LastModified.After(time.Now().Add(-24*time.Hour)) && *getNewFiles == false {
				redisСlient.Del(GSfilename)
				continue
			}
			GSmd5, err := redisСlient.Get(GSfilename).Result()
			if GSmd5 != s3md5 || err == redis.Nil {
				work := MyWork{
					s3Client:     s3Client,
					s3fileName:   s3file.Key,
					s3bucketname: s3bucket,
					GSbucket:     *GSbucketName,
					GSClient:     GSClient,
					count:        &count,
				}

				if err != redis.Nil {
					redisСlient.Del(GSfilename)
				}

				delayFullQueue(workPool, queueCapacity-10)

				if err := workPool.PostWork("routine", &work); err != nil {
					log.Printf("ERROR: %s\n", err)
				}

			} else if err != nil {
				log.Print(err)
			} else {
				redisСlient.Del(GSfilename)
			}
		}
	}
	
	//Wait for empty queue
	for workPool.QueuedWork() > 0 {
		time.Sleep(5 * time.Second)
		log.Println("wait queue")
	}
	log.Println("Finished")
}

func getGSfilename(bucket, filePath string) string {
	return bucket + "/" + filePath
}

func delayFullQueue(WP *workpool.WorkPool, maxObj int32) error {
	for maxObj < WP.QueuedWork() {
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func getS3buckets(Client *minio.Client, exclude, include *string) ([]string, error) {
	var buckets []string
	if *include == "" {
		s3buckets, err := Client.ListBuckets()
		if err != nil {
			return nil, err
		}
		excludeBuckets := make(map[string]bool)
		for _, bucket := range strings.Split(*exclude, ",") {
			excludeBuckets[bucket] = true
		}
		for _, s3bucket := range s3buckets {
			if excludeBuckets[s3bucket.Name] == true {
				continue
			} else {
				buckets = append(buckets, s3bucket.Name)
			}
		}
	} else {
		buckets = strings.Split(*include, ",")
	}
	log.Println("Buckets for process")
	for _, printbucket := range buckets {
		log.Println(printbucket)
	}
	return buckets, nil
}

func (mw *MyWork) DoWork(workRoutine int) {
	//open s3 file
	s3reader, err := mw.s3Client.GetObject(mw.s3bucketname, mw.s3fileName, minio.GetObjectOptions{})
	if err != nil {
		log.Print(mw.s3fileName, err)
	}
	defer s3reader.Close()
	//open GS bucket
	bh := mw.GSClient.Bucket(mw.GSbucket)

	//GS file to open
	GSfilename := getGSfilename(mw.s3bucketname, mw.s3fileName)
	obj := bh.Object(GSfilename)
	ctx := context.Background()
	GSw := obj.NewWriter(ctx)
	defer GSw.Close()

	//s3 file info
	stat, err := s3reader.Stat()
	if err != nil {
		log.Print(err)
		return
	}

	if _, err := io.CopyN(GSw, s3reader, stat.Size); err != nil {
		log.Print(mw.s3fileName, err)
		return
	} else {
		*mw.count++
		if *mw.count%20 == 0 {
			log.Println("bucket:", mw.s3bucketname, "File processed:", *mw.count, "File:", mw.s3fileName)
		}
	}
}

func getGSfileMap(redisСlient *redis.Client, client *storage.Client, ctx context.Context, bucket, prefix, delim string) error {
	it := client.Bucket(bucket).Objects(ctx, &storage.Query{
		Prefix:    prefix,
		Delimiter: delim,
	})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Println(err)
			return err
		}
		if attrs.Name[len(attrs.Name)-1] != '/' {
			err := redisСlient.Set(attrs.Name, hex.EncodeToString(attrs.MD5), 0).Err()
			if err != nil {
				log.Print(err)
				return err
			}
		}
	}
	return nil
}
