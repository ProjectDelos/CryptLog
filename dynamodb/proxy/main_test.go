package main

import (
	"strconv"
	"testing"
)

type Input struct {
	op    RequestType
	idx   int64
	value string

	exIdx int64
	exVal string
}

func integrationInput() []Input {
	input := make([]Input, 0, 10)
	n := 4
	// generate put data to check inserts
	for i := 0; i < n; i++ {
		input = append(input, Input{
			op:    Put,
			idx:   int64(i),
			value: "entry-" + strconv.Itoa(i),
			exIdx: int64(i),
			exVal: "",
		})
		input = append(input, Input{
			op:    Length,
			idx:   0,
			value: "",
			exIdx: int64(i),
			exVal: "",
		})
	}
	// get the data to see if it is all reachable
	for i := 0; i < n; i++ {
		input = append(input, Input{
			op:    Get,
			idx:   int64(i),
			value: "",
			exIdx: 0,
			exVal: "entry-" + strconv.Itoa(i),
		})
	}
	// now add 4 that will fail the conditional check
	for i := 0; i < n; i++ {
		input = append(input, Input{
			op:    Put,
			idx:   int64(i),
			value: "entry-" + strconv.Itoa(i+n),
			exIdx: int64(i + n),
			exVal: "",
		})
		input = append(input, Input{
			op:    Length,
			idx:   0,
			value: "",
			exIdx: int64(i + n),
			exVal: "",
		})
	}
	// now check the newly added n elements to see if they were added where we expected
	for i := 0; i < n; i++ {
		input = append(input, Input{
			op:    Get,
			idx:   int64(i + n),
			value: "",
			exIdx: 0,
			exVal: "entry-" + strconv.Itoa(i),
		})
	}
	return input
}

func TestIntegration(t *testing.T) {
	reset()
	input := integrationInput()
	for _, in := range input {
		switch in.op {
		case Put:
			idx, err := put(in.idx, in.value, true)
			if err != nil {
				t.Fatal("Failed to put:", err)
			}
			if idx != in.exIdx {
				t.Fatalf("Index does not match: %d = %d", idx, in.exIdx)
			}
		case Get:
			v, err := get(in.idx)
			if err != nil {
				t.Fatal("Failed to get:", err)
			}
			if v != in.exVal {
				t.Fatalf("Value does not match: %s = %s", v, in.exVal)
			}
		case Length:
			i, err := length()
			if err != nil {
				t.Fatal("Failed to put:", err)
			}
			if i != in.exIdx {
				t.Fatalf("Length does not match: %d = %d", i, in.exIdx)
			}
		}
	}
	reset()
	v, err := get(0)
	if err == nil {
		t.Error("After resetting value still exists: ", v)
	}
	i, err := length()
	if err == nil {
		t.Error("After resetting length still exists: ", i)
	}
}
