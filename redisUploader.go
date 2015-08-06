package main

import (
	"flag"
	"fmt"
	"gopkg.in/redis.v3"
	"log"
	"os"
	"runtime"
	"strconv"
	"time"
)

// function accepting filename and write it to Cassandra
func writeFileToRedis(iterator int, file string, client *redis.Client, totalSize *int64, startChannel chan int) {
	fmt.Println("Proceeding ", iterator, " file... ")
	// open file
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}

	// get the file size
	stat, err := f.Stat()
	if err != nil {
		log.Fatal(err)
	}

	// create slice of bytes for new files
	bs := make([]byte, stat.Size())

	// read whole file
	_, err = f.Read(bs)

	if err != nil {
		log.Fatal(err)
	}

	//insert data to Redis
	err = client.Set(strconv.Itoa(iterator), string(bs), 0).Err()
	if err != nil {
		log.Println(err)
	}

	// increment total size
	*totalSize += stat.Size()
	startChannel <- 1

}

func main() {

	runtime.GOMAXPROCS(2)

	// define variables
	var totalDuration time.Duration
	var totalSize int64
	fileCountCursor := 1
	startChannel := make(chan int, 11)

	// define input flags
	path := flag.String("path", "./files/", "path to directory with blob files to upload")
	server := flag.String("server", "[::1]:6379", "Redis server to connect, i.e.: [2001:db8:f:ffff:0:0:0:1]:6139")
	db := flag.Int64("db", 0, "database to connect, by default - 0")
	concurent := flag.Int("concurent", 5, "amount of concurent writes")

	flag.Parse()

	// open directory
	dir, err := os.Open(*path)
	if err != nil {
		log.Fatal(err)
	}
	defer dir.Close()

	// read directory to get attributes
	fileInfos, err := dir.Readdir(-1)
	if err != nil {
		log.Fatal(err)
	}

	// get files count
	files_count := len(fileInfos)

	fmt.Println("Total files count is ", files_count)

	// Cluster definition
	client := redis.NewClient(&redis.Options{
		Addr:     *server,
		Password: "",  // no password set
		DB:       *db, // use default DB
	})
	defer client.Close()

	start := time.Now()

	// follow through list of files and write them to Cassandra into blob filed
	for i, fi := range fileInfos {

		if fileCountCursor > *concurent {
			<-startChannel
		}
		go writeFileToRedis(i+1, *path+fi.Name(), client, &totalSize, startChannel)
		fileCountCursor++

	}
	totalDuration = time.Since(start)

	// wait until last concurent files will be sent
	for i := 1; i <= *concurent; i++ {
		<-startChannel
	}

	// printing total for whole test
	fmt.Println("Ready!\nTotal duration is", totalDuration, "\nTotal sent", totalSize, "bytes")
	fmt.Println("Average file size", totalSize/int64(files_count), "bytes")
	fmt.Println("Concurency:", *concurent)
	fmt.Printf("Average speed %f MB/s\n", float64(totalSize)/totalDuration.Seconds()/1024/1024)

}
