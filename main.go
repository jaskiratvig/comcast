package main

import (
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// JSONObject represents the structure of the JSON object
type JSONObject struct {
	ID    int64     `json:"id"`
	Time  time.Time `json:"time"`
	Words []string  `json:"words"`
}

func main() {
	// Define flags
	input := flag.String("input", "", "An S3 URI (s3://{bucket}/{key}) that refers to the source object to be filtered.")
	withID := flag.Int64("with-id", 0, "An integer that contains the id of a JSON object to be selected.")
	fromTime := flag.String("from-time", "", "An RFC3339 timestamp that represents the earliest time of a JSON object to be selected.")
	toTime := flag.String("to-time", "", "An RFC3339 timestamp that represents the latest time of a JSON object to be selected.")
	withWord := flag.String("with-word", "", "A string containing a word that must be contained in words of a JSON objec to be selected.")
	flag.Parse()

	if *input == "" {
		fmt.Println("-input flag is required")
		os.Exit(1)
	}

	// Create an AWS session
	sess := session.Must(session.NewSession())

	// Create an S3 client
	s3Client := s3.New(sess)

	// Get the object from S3
	inputURL := aws.String(*input)
	result, err := s3Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String("maf-sample-data"),
		Key:    inputURL,
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Read the object
	reader, err := gzip.NewReader(result.Body)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Decode the JSON objects
	decoder := json.NewDecoder(reader)
	for {
		var obj JSONObject
		if err := decoder.Decode(&obj); err == io.EOF {
			break
			} else if err != nil {
			fmt.Println(err)
			os.Exit(1)
			}
		
		// Filter the JSON objects based on the flags
		if *withID != 0 && obj.ID != *withID {
			continue
		}

		if *fromTime != "" {
			from, err := time.Parse(time.RFC3339, *fromTime)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			if obj.Time.Before(from) {
				continue
			}
		}

		if *toTime != "" {
			to, err := time.Parse(time.RFC3339, *toTime)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			if obj.Time.After(to) {
				continue
			}
		}

		if *withWord != "" {
			found := false
			for _, word := range obj.Words {
				if word == *withWord {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		// Print the filtered JSON object to stdout
		jsonBytes, err := json.Marshal(obj)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println(string(jsonBytes))
	}
}