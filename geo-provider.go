package main

// Geographic data provider

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/golang/protobuf/proto"
	//	"github.com/paulmach/orb/geojson"
	geojson "github.com/paulmach/go.geojson"

	geo "github.com/synerex/proto_geography"
	pb "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
)

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	geoJsonFile     = flag.String("geojson", "", "GeoJson file")
	label           = flag.String("label", "", "Label of data")
	lines           = flag.String("lines", "", "geojson for lines")
	idnum           = flag.Int("id", 1, "ID of data")
	sxServerAddress string
)

func sendFile(client *sxutil.SXServiceClient, id int, label string, fname string) {

	bytes, err := ioutil.ReadFile(fname)
	if err != nil {
		log.Print("Can't read file:", err)
		panic("load json")
	}

	geodata := geo.Geo{
		Type:  "geojson",
		Id:    int32(id),
		Label: label,
		Data:  bytes,
	}

	out, _ := proto.Marshal(&geodata) // TODO: handle error

	cont := pb.Content{Entity: out}
	smo := sxutil.SupplyOpts{
		Name:  "GeoJson",
		Cdata: &cont,
	}

	_, nerr := client.NotifySupply(&smo)
	if nerr != nil { // connection failuer with current client
		log.Printf("Connection failure", nerr)
	}
}

func loadGeoJSON(fname string) *geojson.FeatureCollection {
	bytes, err := ioutil.ReadFile(fname)
	if err != nil {
		log.Print("Can't read file:", err)
		panic("load json")
	}

	fc, _ := geojson.UnmarshalFeatureCollection(bytes)

	return fc
}

func sendLines(client *sxutil.SXServiceClient, id int, label string, fname string) {

	jsonData := loadGeoJSON(fname)

	fcs := jsonData.Features
	//	type := jsonData.Type
	fclen := len(fcs)
	lines := make([]*geo.Line, 0, fclen)
	log.Printf("Fetures: %d", fclen)
	for i := 0; i < fclen; i++ {
		geom := fcs[i].Geometry
		//		log.Printf("MulitiLine %d: %v", i, geom.)
		if geom.IsMultiLineString() {
			coord := geom.MultiLineString[0]
			ll := len(coord)
			for j := 0; j < ll-1; j++ {
				lines = append(lines, &geo.Line{
					From: []float64{coord[j][0], coord[j][1]},
					To:   []float64{coord[j+1][0], coord[j+1][1]},
				})
			}
		}
		if geom.IsMultiPolygon() {
			//			log.Printf("MultiPolygon %d: %v", i, geom)
			coord := geom.MultiPolygon[0][0]
			ll := len(coord)
			for j := 0; j < ll-1; j++ {
				lines = append(lines, &geo.Line{
					From: []float64{coord[j][0], coord[j][1]},
					To:   []float64{coord[j+1][0], coord[j+1][1]},
				})
			}
			lines = append(lines, &geo.Line{
				From: []float64{coord[ll-1][0], coord[ll-1][1]},
				To:   []float64{coord[0][0], coord[0][1]},
			})

		}

	}

	geodata := geo.Lines{
		Lines: lines,
		Width: 1,
	}

	out, _ := proto.Marshal(&geodata) // TODO: handle error

	cont := pb.Content{Entity: out}
	smo := sxutil.SupplyOpts{
		Name:  "Lines",
		Cdata: &cont,
	}

	_, nerr := client.NotifySupply(&smo)
	if nerr != nil { // connection failuer with current client
		log.Printf("Connection failure", nerr)
	}
}

func main() {
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	channelTypes := []uint32{pbase.GEOGRAPHIC_SVC}
	// obtain synerex server address from nodeserv
	srv, err := sxutil.RegisterNode(*nodesrv, "GeoService", channelTypes, nil)
	if err != nil {
		log.Fatal("Can't register node...")
	}
	log.Printf("Connecting Server [%s]\n", srv)

	sxServerAddress = srv
	client := sxutil.GrpcConnectServer(srv)
	argJSON := fmt.Sprintf("{Client:GeoService}")
	sclient := sxutil.NewSXServiceClient(client, pbase.GEOGRAPHIC_SVC, argJSON)

	if *geoJsonFile != "" {
		sendFile(sclient, *idnum, *label, *geoJsonFile)
	}
	if *lines != "" {
		sendLines(sclient, *idnum, *label, *lines)
	}

	sxutil.CallDeferFunctions() // cleanup!

}
