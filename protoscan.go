package protoscan

/*
Scenario: lets say you have a pbf struct where the an outer message
is simply a repeated list of message like this in the protobuf file

message FeatureCollection {
    repeated Feature Features = 1;
}

This file could have millions of features and you want to read each of them iteratively
and more importantly not read the entire file in at one time to get each feature.

This repository implements a simple api to output the byte array of each protobuf message
using bufio.Scanner, Unmarshaling will be handled on your own of course.

*/

import (
	"bufio"
	"io"
	//"fmt"
)

// the main struct for this repo
type ProtobufScanner struct {
	Scanner        *bufio.Scanner
	BoolVal        bool
	EndBool        bool
	TotalPosition  int
	BufferPosition int
	increment      int
	SizeBuffer 	   int 
}

var SizeBuffer = 64 * 1028 // 64 kb
var SizeBufferLarge = 64 * 1028 * 1028 // 64 mb

// new protobuf scanner
func NewProtobufScanner(ioreader io.Reader) *ProtobufScanner {
	scanner := bufio.NewScanner(ioreader)
	//scanner.Split(split)
	buf := make([]byte, SizeBuffer)
	scanner.Buffer(buf, SizeBuffer)
	scannerval := &ProtobufScanner{Scanner: scanner, BoolVal: true,SizeBuffer:SizeBuffer}
	// the split function that contains the logic for chunking a protobuf
	split := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if len(data) < scannerval.increment {
			token = make([]byte, scannerval.increment)
			copy(token, data[:scannerval.increment])
			//fmt.Println(token)
			advance = len(data)

		} else {
			token = make([]byte, scannerval.increment)
			copy(token, data)
			advance = scannerval.increment

		}
		if atEOF {
			scannerval.EndBool = true
		}
		return
	}

	scannerval.Scanner.Split(split)
	return scannerval
}

// protobuf scanner with size size
func NewProtobufScannerSize(ioreader io.Reader,size_buffer int) *ProtobufScanner {
	scanner := bufio.NewScanner(ioreader)
	//scanner.Split(split)
	buf := make([]byte, size_buffer)
	scanner.Buffer(buf, size_buffer)
	scannerval := &ProtobufScanner{Scanner: scanner, BoolVal: true,SizeBuffer:size_buffer}
	// the split function that contains the logic for chunking a protobuf
	split := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if len(data) < scannerval.increment {
			token = make([]byte, scannerval.increment)
			copy(token, data[:scannerval.increment])
			//fmt.Println(token)
			advance = len(data)

		} else {
			token = make([]byte, scannerval.increment)
			copy(token, data)
			advance = scannerval.increment

		}
		if atEOF {
			scannerval.EndBool = true
		}
		return
	}

	scannerval.Scanner.Split(split)
	return scannerval
}

func (scanner *ProtobufScanner) Reset() {
	scanner.increment = 0
	scanner.BoolVal = true
	scanner.EndBool = false
	scanner.TotalPosition = 0
	scanner.BufferPosition = 0
}

// returns a continutation bool
func (scanner *ProtobufScanner) Scan() bool {

	scanner.Get_Increment(0)
	if scanner.EndBool {
		return false
	}
	return scanner.BoolVal
}

const maxVarintBytes = 10 // maximum Length of a varint

// generic decode var int for getting size
func DecodeVarint(buf []byte) int {
	var x uint64
	var n int
	for shift := uint(0); shift < 64; shift += 7 {
		if n >= len(buf) {
			return 0
		}
		b := uint64(buf[n])
		n++
		x |= (b & 0x7F) << shift
		if (b & 0x80) == 0 {
			return int(x)
		}
	}

	// The number is too large to represent in a 64-bit value.
	return 0
}


// gets an increment
func (scanner *ProtobufScanner) Get_Increment(step int) []byte {
	// ensuring we only prime once
	scanner.TotalPosition += step

	// getting how much buffer is left in each buffer
	buffer_left := scanner.SizeBuffer - scanner.BufferPosition

	if step > scanner.SizeBuffer {
		var newlist []byte
		if scanner.BufferPosition != 0 {
			// toppign off buffer

			scanner.increment = buffer_left
			scanner.BoolVal = scanner.Scanner.Scan()
			newlist = scanner.Scanner.Bytes()[:scanner.increment]
			scanner.BufferPosition = 0

		}

		for len(newlist) != step {
			remaining_bytes := step - len(newlist)
			if remaining_bytes > scanner.SizeBuffer {
				scanner.increment = scanner.SizeBuffer
				scanner.BoolVal = scanner.Scanner.Scan()
				tmpbytes := scanner.Scanner.Bytes()
				newlist = append(newlist, tmpbytes...)

			} else {
				scanner.increment = remaining_bytes
				scanner.BufferPosition = scanner.BufferPosition + scanner.increment
				scanner.BoolVal = scanner.Scanner.Scan()
				tmpbytes := scanner.Scanner.Bytes()[:scanner.increment]
				newlist = append(newlist, tmpbytes...)

			}

		}
		return newlist

	} else {
		var newlist []byte
		if buffer_left > step {
			scanner.increment = step
			scanner.BoolVal = scanner.Scanner.Scan()
			newlist = scanner.Scanner.Bytes()[:scanner.increment]
			scanner.BufferPosition = scanner.BufferPosition + scanner.increment

		} else {
			// toppign off buffer

			scanner.increment = buffer_left
			scanner.BoolVal = scanner.Scanner.Scan()
			newlist = scanner.Scanner.Bytes()[:scanner.increment]
			scanner.increment = step - buffer_left


			buffer_left = 0
			scanner.BufferPosition = scanner.increment
			scanner.BoolVal = scanner.Scanner.Scan()
			tmpbytes := scanner.Scanner.Bytes()[:scanner.increment]
			newlist = append(newlist, tmpbytes...)

		}
		return newlist
	}

	return []byte{}
	//fmt.Println(scanner.Scanner.Bytes())
}

func (scanner *ProtobufScanner) Protobuf() []byte {
	// ignoring header value
	size := scanner.Get_Increment(1)
	// getting sizes
	size = scanner.Get_Increment(1)
	size_bytes := []byte{size[0]}
	for size[0] > 127 {
		size = scanner.Get_Increment(1)
		size_bytes = append(size_bytes, size[0])

	}
	//size_bytes = append(size_bytes,size[0])
	// getting the size of the protobuf
	size_protobuf := int(DecodeVarint(size_bytes))
	//fmt.Println(size_protobuf,size_bytes)
	// getting the protobuf
	return scanner.Get_Increment(size_protobuf)
}

// returns a [2]int of the file positionals within a protobuf
func (scanner *ProtobufScanner) ProtobufIndicies() ([]byte, [2]int) {
	// ignoring header value
	size := scanner.Get_Increment(1)
	// getting sizes
	size = scanner.Get_Increment(1)
	size_bytes := []byte{size[0]}
	for size[0] > 127 {
		size = scanner.Get_Increment(1)
		size_bytes = append(size_bytes, size[0])

	}
	//size_bytes = append(size_bytes,size[0])
	// getting the size of the protobuf
	pos1 := scanner.TotalPosition
	size_protobuf := int(DecodeVarint(size_bytes))
	//fmt.Println(size_protobuf,size_bytes)
	// getting the protobuf

	return scanner.Get_Increment(size_protobuf), [2]int{pos1, scanner.TotalPosition}
}
