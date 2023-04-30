package main

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"path/filepath"
)

type MapTask struct {
	M, R       int    // total number of map and reduce tasks
	N          int    // map task number, 0-based
	SourceHost string // address of host with map input file
}

type ReduceTask struct {
	M, R        int      // total number of map and reduce tasks
	N           int      // reduce task number, 0-based
	SourceHosts []string // addresses of map workers
}

type Pair struct {
	Key   string
	Value string
}

type Interface interface {
	Map(key, value string, output chan<- Pair) error
	Reduce(key string, values <-chan string, output chan<- Pair) error
}

func mapSourceFile(m int) string       { return fmt.Sprintf("map_%d_source.db", m) }
func mapInputFile(m int) string        { return fmt.Sprintf("map_%d_input.db", m) }
func mapOutputFile(m, r int) string    { return fmt.Sprintf("map_%d_output_%d.db", m, r) }
func reduceInputFile(r int) string     { return fmt.Sprintf("reduce_%d_input.db", r) }
func reduceOutputFile(r int) string    { return fmt.Sprintf("reduce_%d_output.db", r) }
func reducePartialFile(r int) string   { return fmt.Sprintf("reduce_%d_partial.db", r) }
func reduceTempFile(r int) string      { return fmt.Sprintf("reduce_%d_temp.db", r) }
func makeURL(host, file string) string { return fmt.Sprintf("http://%s/data/%s", host, file) }

func (task *MapTask) Process(tempdir string, client Interface) error {
	inputFile := filepath.Join(tempdir, mapInputFile(task.N))
	url := makeURL(task.SourceHost, mapSourceFile(task.N))
	err := download(url, inputFile)
	if err != nil {
		return err
	}
	tasks := make([]*sql.Stmt, task.R)
	for i := range tasks {
		db, err := createDatabase(filepath.Join(tempdir, mapOutputFile(task.N, i)))
		if err != nil {
			return err
		}
		something, err := db.Prepare("INSERT INTO pairs (key, value) VALUES (?,?)")
		if err != nil {
			return err
		}
		tasks[i] = something
	}
	defer db.Close()

	db, err := openDatabase(inputFile)
	if err != nil {
		return err
	}
	defer db.Close()
	rows, err := db.Query("SELECT key, value FROM pairs")
	if err != nil {
		return err
	}
	var pt int
	for rows.Next() {
		var key, value string
		err = rows.Scan(&key, &value)
		if err != nil {
			return err
		}
		map_out := make(chan Pair)
		joined := make(chan error)
		go task.write(map_out, joined, tasks, &pt)
		client.Map(key, value, map_out)
		<-joined
	}
	fmt.Printf("Map Tasks Processed: %d\n", pt)
	return nil
}

func (task *MapTask) write(output chan Pair, joined chan error, tasks []*sql.Stmt, pt *int) {
	for pair := range output {
		*pt++
		hash := fnv.New32()
		hash.Write([]byte(pair.Key))
		r := int(hash.Sum32() % uint32(task.R))
		_, err := tasks[r].Exec(pair.Key, pair.Value)
		if err != nil {
			joined <- err
			return
		}
	}
	joined <- nil
}

func (task *ReduceTask) Process(tempdir string, client Interface) error {
	urls := make([]string, task.M)
	for i := 0; i < task.M; i++ {
		urls[i] = makeURL(task.SourceHost[i], mapOutputFile(i, task.N))
	}
	db, err := mergeDatabases(urls, filepath.Join(tempdir, reduceInputFile(task.N)), filepath.Join(tempdir, reduceTempFile(task.N)))
	defer db.Close()
	if err != nil {
		return err
	}
	outputDB, err := createDatabase(filepath.Join(tempdir, reduceOutputFile(task.N)))
	defer outputDB.Close()
	if err != nil {
		return err
	}
	rows, err := db.Query("SELECT key, value FROM pairs ORDER BY key")
	if err != nil {
		return err
	}
	input_chan := make(chan string)
	output_chan := make(chan Pair)
	prev_key := ""
	for rows.Next() {
		var key, value string
		rows.Scan(&key, &value)
		if prev_key != key {
			if prev_key != "" {
				close(input_chan)
				for i := range output_chan {
					_, err = outputDB.Exec("INSERT INTO pairs (key, value) values (?, ?)", i.Key, i.Value)
					if err != nil {
						return err
					}
				}
				input_chan = make(chan string)
				output_chan = make(chan Pair)
			}
			prev_key = key
			go func() {
				client.Reduce(key, input_chan, output_chan)
			}()
			input_chan <- value
		} else {
			input_chan <- value
		}
	}
	close(input_chan)
	for i := range output_chan {
		outputDB.Exec("INSERT INTO pairs (key, value) VALUES (?, ?)", i.Key, i.Value)
	}
	return nil
}
