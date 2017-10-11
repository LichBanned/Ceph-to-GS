package main

import (
	"flag"
	"io"
	"io/ioutil"
	"log"
	"path/filepath"
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
	"gopkg.in/yaml.v2"
)

type MyWork struct {
	s3Client     *minio.Client
	s3fileName   string
	s3bucketname string
	GSbucket     string
	GSClient     *storage.Client
	count        *int
}

type Config struct {
	s3endpoint        string
	s3accessKeyID     string
	s3secretAccessKey string
	s3useSSL          bool
	projectID         string
	GSbucketName      string
	excludeBucket     string
	copyBucket        string
	getNewFiles       bool
	redisHost         string
	redisPass         string
	redisDB           int
	delteOld          bool
}

func main() {
	config := getConfig()
	var queueCapacity int32 = 5000
	count := 0

	runtime.GOMAXPROCS(runtime.NumCPU())

	workPool := workpool.New(runtime.NumCPU(), queueCapacity)

	// Creates a redis Сlient
	redisСlient := redis.NewClient(&redis.Options{
		Addr:     config.redisHost,
		Password: config.redisPass,
		DB:       config.redisDB,
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
	s3Client, err := minio.New(config.s3endpoint, config.s3accessKeyID, config.s3secretAccessKey, config.s3useSSL)
	if err != nil {
		log.Fatalln(err)
	}

	s3buckets, err := getS3buckets(s3Client, config.excludeBucket, config.copyBucket)
	if err != nil {
		log.Fatalln(err)
	}
	for _, s3bucket := range s3buckets {
		redisСlient.FlushAll()
		log.Println("Create GS file map", s3bucket)
		if err := getGSfileMap(redisСlient, GSClient, ctx, config.GSbucketName, s3bucket+"/", ""); err != nil {
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
			if s3file.Key[len(s3file.Key)-1] == '/' {
				continue
			}

			s3md5 := strings.Replace(s3file.ETag, "\"", "", -1)
			GSfilename := getGSfilename(s3bucket, s3file.Key)
			if s3file.LastModified.After(time.Now().Add(-24*time.Hour)) && config.getNewFiles == false {
				redisСlient.Del(GSfilename)
				continue
			}
			GSmd5, err := redisСlient.Get(GSfilename).Result()
			if GSmd5 != s3md5 || err == redis.Nil {
				work := MyWork{
					s3Client:     s3Client,
					s3fileName:   s3file.Key,
					s3bucketname: s3bucket,
					GSbucket:     config.GSbucketName,
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
		if config.delteOld == true {
			if deleted, err := deleteOldFiles(GSClient, redisСlient, config.GSbucketName); err == nil {
				log.Println("Deleted", deleted, "old files from", s3bucket)
			} else {
				log.Println(err)
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

func getS3buckets(Client *minio.Client, exclude, include string) ([]string, error) {
	var buckets []string
	if include == "" {
		s3buckets, err := Client.ListBuckets()
		if err != nil {
			return nil, err
		}
		excludeBuckets := make(map[string]bool)
		for _, bucket := range strings.Split(exclude, ",") {
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
		buckets = strings.Split(include, ",")
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
		if *mw.count%50 == 0 {
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

func getConfig() Config {
	var config Config
	var ok bool
	defaultConfig := Config{
		s3endpoint:        "",
		s3accessKeyID:     "",
		s3secretAccessKey: "",
		s3useSSL:          false,
		projectID:         "",
		GSbucketName:      "",
		excludeBucket:     "",
		copyBucket:        "",
		getNewFiles:       false,
		redisHost:         "127.0.0.1:6379",
		redisPass:         "",
		redisDB:           0,
		delteOld:          false,
	}

	configFile := flag.String("config", "", "yaml config file path")
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

	if *configFile != "" {
		filename, _ := filepath.Abs(*configFile)
		yamlFile, err := ioutil.ReadFile(filename)
		if err != nil {
			log.Fatal(err)
		}
		var confmap map[interface{}]interface{}
		err = yaml.Unmarshal(yamlFile, &confmap)
		if err != nil {
			log.Fatal(err)
		}
		if config.s3endpoint, ok = confmap["s3ep"].(string); ok !=true {
			config.s3endpoint = defaultConfig.s3endpoint
		}
		if config.s3accessKeyID, ok = confmap["s3id"].(string); ok !=true {
			config.s3accessKeyID = defaultConfig.s3accessKeyID
		}
		if config.s3secretAccessKey, ok = confmap["s3key"].(string); ok !=true {
			config.s3secretAccessKey = defaultConfig.s3secretAccessKey
		}
		if config.projectID, ok = confmap["gsproject"].(string); ok !=true {
			config.projectID = defaultConfig.projectID
		}
		if config.GSbucketName, ok = confmap["gsbucket"].(string); ok !=true {
			config.GSbucketName = defaultConfig.GSbucketName
		}
		if config.excludeBucket, ok = confmap["exclude"].(string); ok !=true {
			config.excludeBucket = defaultConfig.excludeBucket
		}
		if config.copyBucket, ok = confmap["copy"].(string); ok !=true {
			config.copyBucket = defaultConfig.copyBucket
		}
		if config.redisHost, ok = confmap["rhost"].(string); ok !=true {
			config.redisHost = defaultConfig.redisHost
		}
		if config.redisPass, ok = confmap["rpass"].(string); ok !=true {
			config.redisPass = defaultConfig.redisPass
		}
		if config.s3useSSL, ok = confmap["s3ssl"].(bool); ok !=true {
			config.s3useSSL = defaultConfig.s3useSSL
		}
		if config.getNewFiles, ok = confmap["getnew"].(bool); ok !=true {
			config.getNewFiles = defaultConfig.getNewFiles
		}
		if config.redisDB, ok = confmap["rdb"].(int); ok !=true {
			config.redisDB = defaultConfig.redisDB
		}
	} else {
		config.s3endpoint = *s3endpoint
		config.s3accessKeyID = *s3accessKeyID
		config.s3secretAccessKey = *s3secretAccessKey
		config.s3useSSL = *s3useSSL
		config.projectID = *projectID
		config.GSbucketName = *GSbucketName
		config.excludeBucket = *excludeBucket
		config.copyBucket = *copyBucket
		config.getNewFiles = *getNewFiles
		config.redisHost = *redisHost
		config.redisPass = *redisPass
		config.redisDB = *redisDB
	}

	if config.projectID == "" || config.s3endpoint == "" || config.GSbucketName == "" {
		log.Fatalf("gsproject, gsbucket, s3ep variables must be set.\n")
	}

	if config.excludeBucket != "" && config.copyBucket != "" {
		log.Fatalf("Not use excludeBucket and copyBucket at same time ")
	}
	return config
}

func deleteOldFiles(GSClient *storage.Client, redisСlient *redis.Client, GSbucket string) (int, error) {
	var cursor uint64
	var key []string
	var err error
	var count int
	ctx := context.Background()
	for {
		key, cursor, err = redisСlient.Scan(cursor, "", 1).Result()
		if err != nil {
			return count, err
		}
		o := GSClient.Bucket(GSbucket).Object(key[0])
		if err := o.Delete(ctx); err != nil {
			log.Println(err)
		} else {
			count++
		}
		if cursor == 0 {
			break
		}
	}
	return count, nil
}
