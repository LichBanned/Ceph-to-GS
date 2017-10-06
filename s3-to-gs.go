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
	s3fileName   string
	GSbucket     string
	s3bucketname string
	filename     string
	GSClient     *storage.Client
	ctx          context.Context
	s3Client     *minio.Client
	WP           *workpool.WorkPool
	count        *int
	GSfilename   string
}

func main() {
	s3endpoint := flag.String("s3endpoint", "", "s3 url")
	s3accessKeyID := flag.String("s3ID", "", "s3 access ID")
	s3secretAccessKey := flag.String("s3Key", "", "s3 access Key")
	s3useSSL := flag.Bool("s3ssl", false, "use ssl for s3")
	projectID := flag.String("GSproject", "", "gcloud projectID")
	GSbucketName := flag.String("GSbucketName", "", "gcloud bucket name")
	excludeBucket := flag.String("exclude", "", "comma separated s3 bucket names to exclude from process")
	getNewFiles := flag.Bool("getNewFiles", false, "Get files modified in 24h")
	redisHost := flag.String("redisHost", "localhost:6379", "redis server address")
	redisPass := flag.String("redisPass", "", "redis password")
	redisDB := flag.Int("redisDB", 0, "redis database")

	flag.Parse()

	excludeBuckets := make(map[string]bool)
	for _, bucket := range strings.Split(*excludeBucket, ",") {
		//log.Print(bucket)
		excludeBuckets[bucket] = true
	}

	if *projectID == "" || *s3endpoint == "" || *GSbucketName == "" {
		log.Fatalf("GSproject, GSbucketName, s3endpoint variables must be set.\n")
	}

	var queueCapacity int32 = 1000
	count := 0

	Rclient := redis.NewClient(&redis.Options{
		Addr:     *redisHost,
		Password: *redisPass, // no password set
		DB:       *redisDB,   // use default DB
	})
	if _, err := Rclient.Ping().Result(); err != nil {
		log.Fatalln(err)
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	workPool := workpool.New(runtime.NumCPU(), queueCapacity)

	ctx := context.Background()

	// Creates a GSClient.
	GSClient, err := storage.NewClient(ctx)
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
		if excludeBuckets[s3bucket.Name] == true {
			continue
		}
		Rclient.FlushAll()
		log.Println("Create GS file map", s3bucket.Name)
		if err := getGSfileMap(Rclient, GSClient, ctx, *GSbucketName, s3bucket.Name+"/", ""); err != nil {
			log.Fatal(err)
		}

		doneCh := make(chan struct{})
		defer close(doneCh)
		log.Println("Compare GS file map to s3 files", s3bucket.Name)
		for s3file := range s3Client.ListObjects(s3bucket.Name, "", true, doneCh) {
			if s3file.Err != nil {
				log.Println(s3file.Err)
				continue
			}
			s3md5 := strings.Replace(s3file.ETag, "\"", "", -1)
			GSfilename := getGSfilename(s3bucket.Name, s3file.Key)
			if s3file.LastModified.After(time.Now().Add(-24*time.Hour)) && *getNewFiles == false {
				Rclient.Del(GSfilename)
				continue
			}
			GSmd5, err := Rclient.Get(GSfilename).Result()
			if GSmd5 != s3md5 || err == redis.Nil {
				work := MyWork{
					s3fileName:   s3file.Key,
					GSbucket:     *GSbucketName,
					s3bucketname: s3bucket.Name,
					ctx:          ctx,
					s3Client:     s3Client,
					GSClient:     GSClient,
					count:        &count,
					GSfilename:   GSfilename,
				}
				if err := delayFullQueue(workPool, queueCapacity-10); err != nil {
					log.Printf("ERROR: %s\n", err)
				}
				if err := workPool.PostWork("routine", &work); err != nil {
					log.Printf("ERROR: %s\n", err)
				}
				Rclient.Del(GSfilename)
			} else if err != nil {
				log.Print(err)
			} else {
				Rclient.Del(GSfilename)
			}
		}
	}
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
	obj := bh.Object(mw.GSfilename)
	GSw := obj.NewWriter(mw.ctx)
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

func getGSfileMap(Rclient *redis.Client, client *storage.Client, ctx context.Context, bucket, prefix, delim string) error {
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
			err := Rclient.Set(attrs.Name, hex.EncodeToString(attrs.MD5), 0).Err()
			if err != nil {
				log.Print(err)
				return err
			}
		}
	}
	return nil
}
