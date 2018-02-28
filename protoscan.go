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
    //"fmt"
    "io"
)

// the main struct for this repo
type ProtobufScanner struct {
    Scanner *bufio.Scanner
}

// new protobuf scanner
func NewProtobufScanner(ioreader io.Reader) *ProtobufScanner {
    scanner := &ProtobufScanner{Scanner:bufio.NewScanner(ioreader)}
    scanner.Scanner.Split(split)
    return scanner
}

// returns a continutation bool
func (scanner *ProtobufScanner) Scan() bool {
    return scanner.Scanner.Scan()
}

// returns the next protobuf message in bytes
func (scanner *ProtobufScanner) Protobuf() []byte {
    return scanner.Scanner.Bytes()
}


const maxVarintBytes = 10 // maximum Length of a varint

// generic decode var int for getting size
func DecodeVarint(buf []byte) (int) {
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

// the state of the protobuf as were only getting data in max 4096 byte chunks
type State int

// constants of state representing an enum essentially
const (
        Tag State = iota
        Size
        Protobuf
)

var state = Tag
var size,protobuf []byte
var position,protobuf_size int

// the split function that contains the logic for chunking a protobuf
func split(data []byte, atEOF bool) (advance int, token []byte, err error) {
    pos := 0
    boolval := false
    for pos < len(data) && boolval == false  {
        byteval := data[pos]

        switch state {
        case Tag:
            // specifically handles the tag case
            state = Size
        case Size:
            // handles getting size of the next protobuf
            for data[pos] > 127 {

                size = append(size,data[pos])
                pos++
            } 
            size = append(size,data[pos])
            protobuf_size = DecodeVarint(size)
            size = []byte{}
            protobuf = make([]byte,protobuf_size)
            position = 0
            state = Protobuf

        case Protobuf:
            // gets the protobuf
            if protobuf_size != 0 {
                protobuf[position] = byteval
                position++
                if position == protobuf_size {
                    token = protobuf
                    state = Tag
                    boolval = true
                    position = 0
                }
            }
        }
        pos++ 
    }
    advance = pos 

    return
}