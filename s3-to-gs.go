package main

import (
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"encoding/hex"
	"github.com/gizak/termui"
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

	GSbucket string
	GSClient *storage.Client

	count *int
	table *termui.Table
}

type Config struct {
	s3endpoint        string
	s3accessKeyID     string
	s3secretAccessKey string
	s3useSSL          bool

	redisHost string
	redisPass string
	redisDB   int

	projectID    string
	GSbucketName string

	excludeBucket string
	copyBucket    string

	getNewFiles bool
	delteOld    bool
}

var config Config

func main() {
	config = getConfig()
	var queueCapacity int32 = 5000
	count := 0

	err := termui.Init()
	if err != nil {
		log.Fatal(err)
	}
	defer termui.Close()
	cTable, rGauge := initTable()

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

	printCurrneJob("Get S3 buckets")
	s3buckets, err := getS3buckets(s3Client, config.excludeBucket, config.copyBucket)
	if err != nil {
		log.Fatalln(err)
	}
	for _, s3bucket := range s3buckets {
		redisСlient.FlushAll()
		printCurrneJob("Create GS file map " + s3bucket)
		if err := getGSfileMap(redisСlient, GSClient, ctx, config.GSbucketName, s3bucket+"/", ""); err != nil {
			log.Fatal(err)
		}
		redisDBfull := redisСlient.DBSize().Val()

		doneCh := make(chan struct{})
		defer close(doneCh)
		printCurrneJob("Compare GS file map to s3 files " + s3bucket)
		//log.Println("Compare GS file map to s3 files", s3bucket)
		for s3file := range s3Client.ListObjects(s3bucket, "", true, doneCh) {
			if s3file.Err != nil {
				printLastErr(s3file.Err.Error())
				//log.Println(s3file.Err)
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
					table:        cTable,
				}

				if err != redis.Nil {
					redisСlient.Del(GSfilename)
				}
				redisDBsize := redisСlient.DBSize().Val()
				printRCache(rGauge, redisDBsize, redisDBfull, s3bucket)

				delayFullQueue(workPool, queueCapacity-10)

				if err := workPool.PostWork("routine", &work); err != nil {
					//log.Printf("ERROR: %s\n", err)
					printLastErr(err.Error())

				}

			} else if err != nil {
				//log.Print(err)
				printLastErr(err.Error())
				if _, err = redisСlient.Ping().Result(); err != nil {
					redisСlient, _ = redisConnect()
				}
			} else {
				redisСlient.Del(GSfilename)
			}
		}
		if config.delteOld == true {
			printCurrneJob("Delete old files from " + s3bucket)
			if deleted, err := deleteOldFiles(GSClient, redisСlient, config.GSbucketName); err == nil {
				//log.Println("Deleted", deleted, "old files from", s3bucket)
				printLastErr("Deleted " + strconv.Itoa(deleted) + " old files from " + s3bucket)
			} else {
				//log.Println(err)
				printLastErr(err.Error())
			}
		}
	}

	for workPool.QueuedWork() > 0 {
		time.Sleep(5 * time.Second)
		printCurrneJob("wait queue")
	}
	printCurrneJob("Finished")
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

	configFile := flag.String("config", "./config.yaml", "yaml config file path")
	s3endpoint := flag.String("s3ep", defaultConfig.s3endpoint, "s3 url")
	s3accessKeyID := flag.String("s3id", defaultConfig.s3accessKeyID, "s3 access ID")
	s3secretAccessKey := flag.String("s3key", defaultConfig.s3secretAccessKey, "s3 access Key")
	s3useSSL := flag.Bool("s3ssl", defaultConfig.s3useSSL, "use ssl for s3")
	projectID := flag.String("gsproject", defaultConfig.projectID, "gcloud projectID")
	GSbucketName := flag.String("gsbucket", defaultConfig.GSbucketName, "gcloud bucket name")
	excludeBucket := flag.String("exclude", defaultConfig.excludeBucket, "comma separated s3 bucket names to exclude from process")
	copyBucket := flag.String("copy", defaultConfig.copyBucket, "comma separated s3 bucket names to process, do not read bucket list from s3")
	getNewFiles := flag.Bool("getnew", defaultConfig.getNewFiles, "Get files modified in 24h")
	redisHost := flag.String("rhost", defaultConfig.redisHost, "redis server address")
	redisPass := flag.String("rpass", defaultConfig.redisPass, "redis password")
	redisDB := flag.Int("rdb", defaultConfig.redisDB, "redis database")
	delteOld := flag.Bool("delteOld", defaultConfig.delteOld, "delete old files")

	flag.Parse()
	if _, err := os.Stat(*configFile); os.IsNotExist(err) {
		//log.Println("No config file", err)
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
		config.delteOld = *delteOld
	} else {
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
		if config.s3endpoint, ok = confmap["s3ep"].(string); ok != true {
			config.s3endpoint = defaultConfig.s3endpoint
		}
		if config.s3accessKeyID, ok = confmap["s3id"].(string); ok != true {
			config.s3accessKeyID = defaultConfig.s3accessKeyID
		}
		if config.s3secretAccessKey, ok = confmap["s3key"].(string); ok != true {
			config.s3secretAccessKey = defaultConfig.s3secretAccessKey
		}
		if config.projectID, ok = confmap["gsproject"].(string); ok != true {
			config.projectID = defaultConfig.projectID
		}
		if config.GSbucketName, ok = confmap["gsbucket"].(string); ok != true {
			config.GSbucketName = defaultConfig.GSbucketName
		}
		if config.excludeBucket, ok = confmap["exclude"].(string); ok != true {
			config.excludeBucket = defaultConfig.excludeBucket
		}
		if config.copyBucket, ok = confmap["copy"].(string); ok != true {
			config.copyBucket = defaultConfig.copyBucket
		}
		if config.redisHost, ok = confmap["rhost"].(string); ok != true {
			config.redisHost = defaultConfig.redisHost
		}
		if config.redisPass, ok = confmap["rpass"].(string); ok != true {
			config.redisPass = defaultConfig.redisPass
		}
		if config.s3useSSL, ok = confmap["s3ssl"].(bool); ok != true {
			config.s3useSSL = defaultConfig.s3useSSL
		}
		if config.getNewFiles, ok = confmap["getnew"].(bool); ok != true {
			config.getNewFiles = defaultConfig.getNewFiles
		}
		if config.redisDB, ok = confmap["rdb"].(int); ok != true {
			config.redisDB = defaultConfig.redisDB
		}
		if config.delteOld, ok = confmap["delteOld"].(bool); ok != true {
			config.delteOld = defaultConfig.delteOld
		}
	}

	if config.projectID == "" || config.s3endpoint == "" || config.GSbucketName == "" {
		log.Fatalf("gsproject, gsbucket, s3ep variables must be set.\n")
	}

	if config.excludeBucket != "" && config.copyBucket != "" {
		log.Fatalf("Not use excludeBucket and copyBucket at same time ")
	}
	return config
}

func getGSfilename(bucket, filePath string) string {
	return bucket + "/" + filePath
}

func delayFullQueue(WP *workpool.WorkPool, maxObj int32) error {
	printQueueSize(int(WP.QueuedWork()))
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
	printBuckets(buckets)
	return buckets, nil
}

func (mw *MyWork) DoWork(workRoutine int) {
	//open s3 file
	s3reader, err := mw.s3Client.GetObject(mw.s3bucketname, mw.s3fileName, minio.GetObjectOptions{})
	if err != nil {
		printLastErr(mw.s3fileName + ": " + err.Error())
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
		printLastErr(err.Error())
		return
	}

	if _, err := io.CopyN(GSw, s3reader, stat.Size); err != nil {
		printLastErr(mw.s3fileName + ": " + err.Error())
		return
	} else {
		*mw.count++
		if *mw.count%50 == 0 {
			printCount(mw.table, *mw.count, mw.s3bucketname, mw.s3fileName)
		}
	}
}

func getGSfileMap(redisСlient *redis.Client, client *storage.Client, ctx context.Context, bucket, prefix, delim string) error {
	count := 0
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
			printLastErr(err.Error())
			return err
		}
		if attrs.Name[len(attrs.Name)-1] != '/' {
			err := redisСlient.Set(attrs.Name, hex.EncodeToString(attrs.MD5), 0).Err()
			if err != nil {
				printLastErr(err.Error())
				if _, err = redisСlient.Ping().Result(); err != nil {
					redisСlient, _ = redisConnect()
				}
			} else {
				count++
				if count%20 == 0 {
					printGScount(count)
				}
			}
		}
	}
	return nil
}

func deleteOldFiles(GSClient *storage.Client, redisСlient *redis.Client, GSbucket string) (int, error) {
	var cursor uint64
	var key []string
	var err error
	cursor = 0
	count := 0
	ctx := context.Background()
	for {
		key, cursor, err = redisСlient.Scan(cursor, "", 10).Result()
		if err != nil || len(key) == 0 {
			return count, err
		}
		for i := 0; i < len(key); i++ {
			o := GSClient.Bucket(GSbucket).Object(key[i])
			if err := o.Delete(ctx); err != nil {
				printLastErr(err.Error())
			} else {
				redisСlient.Del(key[i])
				count++
				if count%20 == 0 {
					printGScount(int(redisСlient.DBSize().Val()))
				}
			}
		}
		if redisСlient.DBSize().Val() == 0 {
			break
		}
	}
	return count, nil
}

func redisConnect() (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     config.redisHost,
		Password: config.redisPass,
		DB:       config.redisDB,
	})
	_, err := client.Ping().Result()
	for err != nil {
		printLastErr("Redis reconnect: " + err.Error())
		client = redis.NewClient(&redis.Options{
			Addr:     config.redisHost,
			Password: config.redisPass,
			DB:       config.redisDB,
		})
		time.Sleep(5 * time.Second)
		_, err = client.Ping().Result()
	}
	return client, nil
}

func initTable() (*termui.Table, *termui.Gauge) {
	rows := [][]string{
		[]string{"Count", "Current bucket", "Current file"},
		[]string{"0", "                            ", "                                                                             "},
	}

	table := termui.NewTable()
	table.Rows = rows
	table.FgColor = termui.ColorWhite
	table.BgColor = termui.ColorDefault
	table.TextAlign = termui.AlignCenter
	table.Separator = false
	table.Analysis()
	table.SetSize()
	table.Y = 0
	table.X = 0
	table.Border = false

	Gauge := termui.NewGauge()
	Gauge.Percent = 0
	Gauge.Width = 50
	Gauge.Height = 3
	Gauge.Y = 3
	Gauge.BorderLabel = "Redis Cache search remain"
	Gauge.Label = "{{percent}}% (0/0)"
	Gauge.LabelAlign = termui.AlignRight

	termui.Render(table, Gauge)

	return table, Gauge
}

func printCount(table *termui.Table, count int, bucket, file string) {
	table.Rows = [][]string{
		[]string{"Count", "Current bucket", "Current file"},
		[]string{strconv.Itoa(count), bucket, file},
	}
	termui.Render(table)
}

func printRCache(Gauge *termui.Gauge, size, full int64, bucket string) {
	if size > 0 && full > 0 {
		Gauge.Percent = int((size * 100) / full)
		Gauge.BorderLabel = "Redis " + bucket + " search remain"
		Gauge.Label = "{{percent}}% (" + strconv.FormatInt(size, 10) + "/" + strconv.FormatInt(full, 10) + ")"
		termui.Render(Gauge)
	} else {
		Gauge.Percent = 0
		Gauge.Label = "{{percent}}% (0/0)"
		termui.Render(Gauge)
	}
}

func printBuckets(list []string) {
	ls := termui.NewList()
	ls.Items = list
	ls.ItemFgColor = termui.ColorYellow
	ls.BorderLabel = "Buckets to process"
	ls.Height = len(list)
	ls.Width = 25
	ls.Y = 6

	termui.Render(ls)
}

func printCurrneJob(job string) {
	par := termui.NewPar(job)
	par.BorderLabel = "current job"
	par.Height = 3
	par.Width = 50
	par.Y = 6
	par.X = 27
	//par.Border = false

	termui.Render(par)
}

func printLastErr(lerr string) {
	par := termui.NewPar(lerr)
	par.BorderLabel = "last error"
	par.Height = 3
	par.Width = 50
	par.Y = 9
	par.X = 27
	//par.Border = false

	termui.Render(par)
}

func printGScount(count int) {
	par := termui.NewPar(strconv.Itoa(count))
	par.BorderLabel = "Redis DB size"
	par.Height = 3
	par.Width = 20
	par.Y = 3
	par.X = 51
	//par.Border = false

	termui.Render(par)
}

func printQueueSize(count int) {
	par := termui.NewPar(strconv.Itoa(count))
	par.BorderLabel = "Queue size"
	par.Height = 3
	par.Width = 20
	par.Y = 3
	par.X = 72
	//par.Border = false

	termui.Render(par)
}
