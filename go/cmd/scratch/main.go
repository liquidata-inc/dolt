package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"strings"

	"google.golang.org/grpc"

	"github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage"
	"github.com/dolthub/dolt/go/libraries/doltcore/grpcendpoint"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
)

const TestHash = "vgsip4q0b2ngsq19voo61anum0nfh2c5"

func main() {
	fmt.Printf("yay!\n")

	ctx := context.Background()
	denv := env.Load(ctx, env.GetCurrentUserHomeDir, filesys.LocalFS, doltdb.LocalDirDoltDB, "0.0.1")
	if denv == nil {
		panic("no denv...")
	}

	endpoint, opts, err := denv.GetGRPCDialParams(grpcendpoint.Config{
		Endpoint: "doltremoteapi.awsdev.ld-corp.com",
		WithEnvCreds: true,
	})
	if err != nil {
		panic(err)
	}
	cc, err := grpc.Dial(endpoint, opts...)
	if err != nil {
		panic(err)
	}
	fmt.Printf("cc: %v\n", cc)
	client := remotesapi.NewChunkStoreServiceClient(cc)
	fmt.Printf("client: %v\n", client)
	_, err = client.GetRepoMetadata(ctx, &remotesapi.GetRepoMetadataRequest{
		RepoId: &remotesapi.RepoId{
			Org: "reltuk",
			RepoName: "votes_testing",
		},
		ClientRepoFormat: &remotesapi.ClientRepoFormat{
			NbfVersion: "__LD_1__",
			NbsVersion: "5",
		},
	})
	if err != nil {
		panic(err)
	}
	hashesStr, err := ioutil.ReadFile("hashes.txt")
	if err != nil {
		panic(err)
	}
	hashes := strings.Split(string(hashesStr), "\n")
	hashes = hashes[0:len(hashes)-1]
	hashesBytes := make([][]byte, len(hashes))
	for i, h := range hashes {
		parsed := hash.Parse(h)
		hashesBytes[i] = parsed[:]
	}
	locs, err := client.GetDownloadLocations(ctx, &remotesapi.GetDownloadLocsRequest{
		RepoId: &remotesapi.RepoId{
			Org: "reltuk",
			RepoName: "votes_testing",
		},
		ChunkHashes: hashesBytes,
	})
	if err != nil {
		panic(err)
	}
	for _, l := range locs.Locs {
		gr := remotestorage.GetRange(*l.GetHttpGetRange())
		gr.Sort()
		fmt.Printf("gr.NumChunks(): %v\n", gr.NumChunks())
		aggregated := remotestorage.AggregateDownloads(64 * 1024, map[string]*remotestorage.GetRange{"testing": &gr})
		fmt.Printf("len(aggregated): %v\n", len(aggregated))
		for _, a := range aggregated {
			fmt.Printf("offset: %d, length: %d\n", a.ChunkStartOffset(0), a.RangeLen())
		}
	}
}
