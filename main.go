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
	f, err := os.Open("./data/bremen-latest.osm.pbf")
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	d := osmpbf.NewDecoder(f)

	d.SetBufferSize(osmpbf.MaxBlobSize)

	err = d.Start(runtime.GOMAXPROCS(-1))
	if err != nil {
		log.Fatal(err)
	}

	var nc, wc, rc uint64

	var nodes []OsmNodePosition

	for {
		if v, err := d.Decode(); err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		} else {
			switch v := v.(type) {
			case *osmpbf.Node:
				item := OsmNodePosition{v.ID, int32(v.Lat * 10000000), int32(v.Lon * 10000000)}
				nodes = append(nodes, item)
				nc++
			case *osmpbf.Way:

				wc++
			case *osmpbf.Relation:

				rc++
			default:
				log.Fatalf("unknown type %T\n", v)
			}
		}
	}

	fmt.Printf("Nodes: %d, Ways: %d, Relations: %d\n", nc, wc, rc)
	fmt.Printf("Structs in nodes slice: %d\n", len(nodes))
}

type OsmNodePosition struct {
	Id  int64
	lat int32
	lon int32
}
