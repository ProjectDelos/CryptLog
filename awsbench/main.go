package main

import (
	"os"
	"os/exec"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
)

func main() {
	// double check that everything has been made
	os.Chdir("../")
	cmd := exec.Command("make")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		panic(err)
	}

	// copy over client and proxy to AWS to run n clients
	svc := ec2.New(session.New(), &aws.Config{Region: aws.String("us-west-2")})

	// check the status of existing instances
	params := &ec2.DescribeInstancesInput{
		Filters: []*ec2.Filter{
			&ec2.Filter{
				Name: aws.String("instance-state-name"),
				Values: []*string{
					aws.String("running"),
					aws.String("pending"),
				},
			},
		},
	}

	// TODO: Actually care if we can't connect to a host
	resp, _ := svc.DescribeInstances(params)
	// copy over server to AWS to run VM if specified

	// start up server

	// start up clients

}
