package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"
)

const tempdir = "tmp"
const addr = "localhost:8080"

type Client struct{}

func (c Client) Map(key, value string, output chan<- Pair) error {
	defer close(output)
	lst := strings.Fields(value)
	for _, elt := range lst {
		word := strings.Map(func(r rune) rune {
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				return unicode.ToLower(r)
			}
			return -1
		}, elt)
		if len(word) > 0 {
			output <- Pair{Key: word, Value: "1"}
		}
	}
	return nil
}

func (c Client) Reduce(key string, values <-chan string, output chan<- Pair) error {
	defer close(output)
	count := 0
	for v := range values {
		i, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		count += i
	}
	p := Pair{Key: key, Value: strconv.Itoa(count)}
	output <- p
	return nil
}

func main() {

	ex, err := os.Executable()
	if err != nil {
		log.Fatalf("Error getting executable path: %v", err)
	}

	path := filepath.Dir(ex)
	source := path + "/austen.db"
	m := 9
	r := 3
	outputFiles := make([]string, m)

	// Create temporary directory and defer cleanup
	tempdir, err := ioutil.TempDir("", "mapreduce")
	if err != nil {
		log.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempdir)

	// check if split is successful
	err = splitDatabase(source, outputFiles)
	if err != nil {
		log.Fatalf("Issue splitting database: %v", err)
	}

	go Service(tempdir, addr)

	client := Client{}
	for i := 0; i < m; i++ {
		log.Printf("%d Map", i)
		mapTask := MapTask{
			M:          m,
			R:          r,
			N:          i,
			SourceHost: "/" + outputFiles[i],
		}

		mapTask.Process(&client)
	}

	// Process all reduce tasks
	for i := 0; i < r; i++ {
		log.Printf("%d Reduce", i)
		reduceTask := ReduceTask{
			M:            m,
			R:            r,
			N:            i,
			OutputPrefix: "mr-out-",
		}
		reduceTask.Process(&client)
	}

	// Merge reduce outputs
	reduceOutputFiles := make([]string, r)
	for i := 0; i < r; i++ {
		reduceOutputFiles[i] = fmt.Sprintf("mr-out-%d-%d", i, i)
	}
	outputFile := "mr-out.txt"
	if err := mergeDatabases(reduceOutputFiles, outputFile); err != nil {
		log.Fatalf("Error merging reduce outputs")

		/*paths := make([]string, m)
		for i := 0; i < m; i++ {
			paths[i] = makeURL(addr, outputFiles[i])
		}

		// check if merge is successful
		db, err := mergeDatabases(paths, "merged.db", "temp.db")
		if err != nil {
			log.Fatalf("Issue with merged databases: %v", err)
		}
		log.Print(db)*/
	}

}

func Service(splitDir, addr string) {
	http.Handle("/"+splitDir+"/", http.StripPrefix("/"+splitDir, http.FileServer(http.Dir(splitDir))))
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Printf("Error in HTTP server for %s: %v", addr, err)
	}
}

func makeURL(host, file string) string {
	return fmt.Sprintf("http://%s/tmp/%s", host, file)
}
