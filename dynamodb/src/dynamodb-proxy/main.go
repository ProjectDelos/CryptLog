package main

import (
	"encoding/json"
	"errors"
)
import "flag"
import "fmt"
import "log"
import "net"
import "reflect"
import "strconv"

import "github.com/aws/aws-sdk-go/aws"
import "github.com/aws/aws-sdk-go/aws/awserr"
import "github.com/aws/aws-sdk-go/aws/session"
import "github.com/aws/aws-sdk-go/service/dynamodb"

var svc *dynamodb.DynamoDB
var table string
var testMode bool

func init() {
	flag.BoolVar(&testMode, "test", false, "run on test table")
	ses := session.New(&aws.Config{Region: aws.String("us-west-2")})
	svc = dynamodb.New(ses, aws.NewConfig().WithRegion("us-west-2"))
}

type dbEntry struct {
	Index int64
	Data  string
}

func dbEntryFromItem(item map[string]*dynamodb.AttributeValue) (dbEntry, error) {
	if item["index"] == nil {
		return dbEntry{-1, ""}, errors.New("Entry does not exist")
	}

	index, err := strconv.ParseInt(*item["index"].N, 10, 64)
	if err != nil {
		log.Panicf("error parsing dbEntry: %s\n", err)
	}

	return dbEntry{
		Index: index,
		Data:  *item["data"].S,
	}, err
}

func lengthFromItem(item map[string]*dynamodb.AttributeValue) (int64, error) {
	if item["QueueLength"] == nil {
		return 0, nil
	}

	length, err := strconv.ParseInt(*item["QueueLength"].N, 10, 64)
	if err != nil {
		log.Panicf("error parsing QueueLength: %s\n", err)
	}

	return length, err
}

func del(index int64) error {
	log.Println("DELETING INDEX:", index)
	// i := "index"
	params := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"index": {N: aws.String(strconv.FormatInt(index, 10))},
		},
		TableName: aws.String(table),
	}
	resp, err := svc.DeleteItem(params)
	log.Println("deletion:", resp, err)
	return err
}

func get(index int64) (string, error) {
	log.Println("GETTING: ", index)
	params := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"index": {N: aws.String(strconv.FormatInt(index, 10))},
		},
		TableName:      aws.String(table),
		ConsistentRead: aws.Bool(true),
	}
	resp, err := svc.GetItem(params)

	if err != nil {
		log.Println("GET: Err:", err.Error())
		return "", err
	}

	fmt.Println("GET: RESP:", resp)
	entry, err := dbEntryFromItem(resp.Item)
	fmt.Println("entry:", entry, err)
	if err == nil && index != entry.Index {
		log.Panicf("retreived entry index that does not match: %d != %d\n", index, entry.Index)
	}
	return entry.Data, err
}

func put(index int64, data string, conditional bool) (error, bool) {
	log.Println("PUT:", index, data, conditional)
	i := "index"
	params := &dynamodb.PutItemInput{
		TableName: aws.String(table), // Required
		Item: map[string]*dynamodb.AttributeValue{
			"index": {N: aws.String(strconv.FormatInt(index, 10))},
			"data":  {S: aws.String(data)},
		},
	}
	if conditional {
		params.ExpressionAttributeNames = map[string]*string{
			"#index": &i,
		}
		params.ConditionExpression = aws.String("attribute_not_exists(#index)")
	}
	resp, err := svc.PutItem(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		if ae, ok := err.(awserr.RequestFailure); ok {
			if ae.Code() == "ConditionalCheckFailedException" {
				log.Println("Failed conditional check: caught:", resp, err)
				return err, true
			}
		}
		log.Println("PUT: ERR:", reflect.TypeOf(err), err.Error())
		return err, false
	}
	log.Println("PUT: RESP:", resp)
	return nil, false
}

func updateLength() error {
	params := &dynamodb.UpdateItemInput{
		TableName: aws.String(table),
		Key: map[string]*dynamodb.AttributeValue{
			"QueueLength": {
				S: aws.String("QueueLength"),
			},
		},
		UpdateExpression: aws.String("ADD Length 1"),
	}
	_, err := svc.UpdateItem(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		log.Println("Failed updateLength:", err.Error())
		return err
	}
	return nil
}

func length() (int64, error) {
	log.Println("Getting Length")
	params := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"QueueLength": {
				S: aws.String("QueueLength"),
			},
		},
		TableName:      aws.String(table),
		ConsistentRead: aws.Bool(true),
	}
	resp, err := svc.GetItem(params)

	if err != nil {
		log.Println("GET: Err: assuming length = 0", err.Error())
		return 0, nil
	}

	fmt.Println("GET: RESP:", resp)
	l, err := lengthFromItem(resp.Item)
	fmt.Println("l:", l, err)
	return l, nil
}

// RequestType is the type of request
type RequestType int64

// Put: dynamodb put request
const (
	Put    RequestType = 0
	Get                = 1
	Delete             = 2
	Length             = 3
)

// Request for log
type Request struct {
	RequestNumber int64       `json:"request_number"`
	RequestType   RequestType `json:"request_type"`
	Conditional   bool        `json:"conditional"`
	Index         int64       `json:"index"`
	Data          string      `json:"data"`
}

// Response for log
type Response struct {
	RequestNumber int64  `json:"request_number"`
	Index         int64  `json:"index"`
	Data          string `json:"data"`
	Length        int64  `json:"length"`
	Err           string `json:"error"`
	ValidationErr bool   `json:"validation_error"`
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	d := json.NewDecoder(conn)
	e := json.NewEncoder(conn)

	for {
		var req Request
		var resp Response

		err := d.Decode(&req)
		if err != nil {
			log.Println("Error decoding:", err)
			return
		}

		resp.RequestNumber = req.RequestNumber
		resp.Index = req.Index

		switch req.RequestType {
		case Put:
			var valErr bool
			for {
				err, valErr = put(req.Index, req.Data, req.Conditional)
				// there was a validation error
				if err != nil && !valErr {
					break
				}
				// it was successful
				if err == nil {
					break
				}
				// it was a validation error: try the next index
				req.Index++
				resp.Index++
			}
			resp.ValidationErr = false
			if err != nil {
				// update the length field of the queue
				err = updateLength()
			}
		case Get:
			var data string
			data, err = get(req.Index)
			resp.Data = data
		case Delete:
			err = del(req.Index)
		case Length:
			var l int64
			l, err = length()
			resp.Length = l
		}
		if err != nil {
			resp.Err = err.Error()
		}
		e.Encode(resp)
	}
}

func main() {
	flag.Parse()
	table = "cryptlog"
	if testMode {
		table = "cryptlog_test"
	}
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			// handle error
			log.Println("ERROR: Error accepting tcp connection:", err)
		}
		go handleConnection(conn)
	}
}
