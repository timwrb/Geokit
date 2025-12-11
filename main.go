package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"runtime"

	"github.com/qedus/osmpbf"
)

func main() {
	file, err := os.Open("./data/bremen-latest.osm.pbf")
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	decoder := osmpbf.NewDecoder(file)

	decoder.SetBufferSize(osmpbf.MaxBlobSize)

	err = decoder.Start(runtime.GOMAXPROCS(-1))
	if err != nil {
		log.Fatal(err)
	}

	var nodeCount, wayCount, relCount uint64

	var nodes []OsmNodePosition

	for {
		if v, err := decoder.Decode(); err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		} else {
			switch v := v.(type) {
			case *osmpbf.Node:
				item := OsmNodePosition{v.ID, int32(v.Lat * 10000000), int32(v.Lon * 10000000)}
				nodes = append(nodes, item)
				nodeCount++
			case *osmpbf.Way:

				wayCount++
			case *osmpbf.Relation:

				relCount++
			default:
				log.Fatalf("unknown type %T\n", v)
			}
		}
	}

	fmt.Printf("Nodes: %d, Ways: %d, Relations: %d\n", nodeCount, wayCount, relCount)
	fmt.Printf("Structs in nodes slice: %d\n", len(nodes))
}

type OsmNodePosition struct {
	Id  int64
	lat int32
	lon int32
}
