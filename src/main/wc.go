package main

import (
	"fmt"
	"mapreduce"
	"os"
	//
	"strings"
	"strconv"
	"unicode"
)

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
func mapF(filename string, contents string) []mapreduce.KeyValue {
	// Your code here (Part II).

	// Your mapF() will be passed the name of a file, as well as that file's contents;
	// it should split the contents into words, and return a Go slice of mapreduce.KeyValue.

	// While you can choose what to put in the keys and values for the mapF output,
	// for word count it only makes sense to use words as the keys.

	// Seperate the contents into strings first.
	seps := strings.FieldsFunc(contents, func(r rune) bool {
		return !unicode.IsLetter(r)
		})

	// fmt.Println("mapF receives %d words from %s", len(seps), filename)

	counts := make(map[string] int)

	for _, word := range seps {
		counts[word]++
	}

	result := make([]mapreduce.KeyValue, len(counts))

	i := 0
	for word, times := range counts {
		result[i] = mapreduce.KeyValue{word, strconv.Itoa(times)}
		i++
		// fmt.Println("Word:%s, count:%d",word, strconv.Itoa(times))
	}

	return result
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
func reduceF(key string, values []string) string {
	// Your code here (Part II).

	// Your reduceF() will be called once for each key, with a slice of all the values
	// generated by mapF() for that key. It must return a string containing the total
	// number of occurences of the key.

	sum := 0
	for _, times := range values {
		num, err := strconv.Atoi(times)
		if err != nil {
			fmt.Println("Error, the count string cannot be converted to integer.")
		}
		sum += num
	}

	return strconv.Itoa(sum)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("wcseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("wcseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100, nil)
	}
}
