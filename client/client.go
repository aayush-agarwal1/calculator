package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/aayush-agarwal1/calculator/calculatorpb"
	"google.golang.org/grpc"
)

func main() {

	fmt.Println("Beginning sending requests")

	cc, err := grpc.Dial("localhost:8080", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}
	defer cc.Close()

	c := calculatorpb.NewCalculatorServiceClient(cc)

	//Unary API function - Calculator
	Sum(c)

	// Server-Side Streaming function - PrimeNumbers
	PrimeNumbers(c)

	//Client-Side Streaming function - ComputeAverage
	ComputeAverage(c)

	//bidirectional streaming function - FindMaxNumber
	FindMaxNumber(c)

	fmt.Println("Completed calling all APIS")

}

func Sum(c calculatorpb.CalculatorServiceClient) {

	fmt.Println("Starting to do a unary GRPC....")

	req := calculatorpb.SumRequest{
		Num1: 5.5,
		Num2: 7.4,
	}

	resp, err := c.Sum(context.Background(), &req)
	if err != nil {
		log.Fatalf("error while calling sum grpc unary call: %v", err)
	}

	log.Printf("Response from Unary Call, Sum : %v", resp.GetSum())

}

func PrimeNumbers(c calculatorpb.CalculatorServiceClient) {
	fmt.Println("Staring ServerSide GRPC streaming ....")

	req := calculatorpb.PrimeNumbersRequest{
		Limit: 15,
	}

	respStream, err := c.PrimeNumbers(context.Background(), &req)
	if err != nil {
		log.Fatalf("error while calling Prime Numbers server-side streaming grpc : %v", err)
	}

	for {
		msg, err := respStream.Recv()
		if err == io.EOF {
			//we have reached to the end of the file
			break
		}

		if err != nil {
			log.Fatalf("error while receving server stream : %v", err)
		}

		log.Println("Response From Server, Prime Number : ", msg.GetPrimeNum())
	}
}

func ComputeAverage(c calculatorpb.CalculatorServiceClient) {

	fmt.Println("Starting Client Side Streaming over GRPC ....")

	stream, err := c.ComputeAverage(context.Background())
	if err != nil {
		log.Fatalf("error occured while performing client-side streaming : %v", err)
	}

	requests := []*calculatorpb.ComputeAverageRequest{
		{
			Num: 10,
		},
		{
			Num: 16,
		},
		{
			Num: 20,
		},
		{
			Num: 14,
		},
	}

	for _, req := range requests {
		log.Println("Sending Request.... : ", req)
		stream.Send(req)
		time.Sleep(1 * time.Second)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Error while receiving response from server : %v", err)
	}
	log.Println("Response From Server, Average: ", resp.GetAvg())
}

func FindMaxNumber(c calculatorpb.CalculatorServiceClient) {
	fmt.Println("Starting Bi-directional stream by calling Find Max Number over GRPC......")

	requests := []*calculatorpb.FindMaxNumberRequest{
		{
			Num: 1,
		},
		{
			Num: 3,
		},
		{
			Num: 5,
		},
		{
			Num: 4,
		},
		{
			Num: 8,
		},
	}

	stream, err := c.FindMaxNumber(context.Background())
	if err != nil {
		log.Fatalf("error occured while performing client side streaming : %v", err)
	}

	//wait channel to block receiver
	waitchan := make(chan struct{})

	go func(requests []*calculatorpb.FindMaxNumberRequest) {
		for _, req := range requests {

			log.Println("Sending Request..... : ", req.GetNum())
			err := stream.Send(req)
			if err != nil {
				log.Fatalf("error while sending request to CalculatorEveryone service : %v", err)
			}
			time.Sleep(1000 * time.Millisecond)
		}
		stream.CloseSend()
	}(requests)

	go func() {
		for {

			resp, err := stream.Recv()
			if err == io.EOF {
				close(waitchan)
				return
			}

			if err != nil {
				log.Fatalf("error receiving response from server : %v", err)
			}

			log.Printf("Response From Server, Max : %v\n", resp.GetMax())
		}
	}()

	//block until everything is finished
	<-waitchan
}
