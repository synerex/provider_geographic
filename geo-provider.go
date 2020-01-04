package main

// Geographic data provider

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/golang/protobuf/proto"
	geo "github.com/synerex/proto_geographic"
	pb "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
)

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	geoJsonFile     = flag.String("geojson", "", "GeoJson file")
	label           = flag.String("label", "", "Label of data")
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
		Name:  "GeoData",
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
	argJson := fmt.Sprintf("{Client:GeoService}")
	sclient := sxutil.NewSXServiceClient(client, pbase.GEOGRAPHIC_SVC, argJson)

	if *geoJsonFile != "" {
		sendFile(sclient, *idnum, *label, *geoJsonFile)
	}

	sxutil.CallDeferFunctions() // cleanup!

}
