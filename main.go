package main

import (
	"context"
	"encoding/binary"
	"log"
	"time"

	"github.com/spf13/cobra"
	etcdv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := etcdv3.New(etcdv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		log.Fatalf("failed to create etcd client: %q", err)
	} else {
		log.Println("client ok!")
	}

	defer cli.Close()

	clientCtx, clientCancel := context.WithTimeout(ctx, time.Second*5)
	defer clientCancel()

	run(clientCtx, cli)
}

func run(ctx context.Context, cli *etcdv3.Client) {
	var rootCmd = &cobra.Command{
		Use:   "sem",
		Short: "Sem abstracts a semaphore over etcd.",
	}
	var acquireCmd = &cobra.Command{
		Use:   "acquire",
		Short: "acquire adds a holder a semaphore resource.",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Do Stuff Here
			acquire(ctx, cli)
			return nil
		},
	}
	var releaseCmd = &cobra.Command{
		Use:   "release",
		Short: "Release removes a holder from a semaphore resource.",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Do Stuff Here
			release(ctx, cli)
			return nil
		},
	}
	rootCmd.AddCommand(acquireCmd)
	rootCmd.AddCommand(releaseCmd)
	rootCmd.Execute()
}

func acquire(ctx context.Context, cli *etcdv3.Client) {
	path := "lock"

	res, err := cli.Get(ctx, path)
	if err != nil {
		log.Fatalf("failed to get key: %q", err)
	}

	var data = make([]byte, 8)
	var val uint64 = 1

	if len(res.Kvs) > 0 && res.Kvs[0] != nil {
		log.Println("found val")

		copy(data, res.Kvs[0].Value)
		curr := binary.LittleEndian.Uint64(data)

		log.Printf("src val: '%d'", curr)
		// NOTE: can overflow
		val = curr + 1
	} else {
		log.Println("no value for key")
	}

	getRevision := res.Header.GetRevision()
	binary.LittleEndian.PutUint64(data, val)

	txCreate := etcdv3.OpTxn(
		nil,
		[]etcdv3.Op{
			etcdv3.OpPut(path, string(data)),
			etcdv3.OpGet(path),
		},
		nil,
	)

	txUpdate := etcdv3.OpTxn(
		[]etcdv3.Cmp{etcdv3.Compare(etcdv3.ModRevision(path), "=", getRevision)},
		[]etcdv3.Op{
			etcdv3.OpPut(path, string(data)),
			etcdv3.OpGet(path),
		},
		nil,
	)

	txnCtx, txnCancel := context.WithTimeout(ctx, time.Second*5)
	defer txnCancel()

	txn, err := cli.Txn(txnCtx).If(
		etcdv3.Compare(etcdv3.CreateRevision(path), "=", 0),
	).Then(
		txCreate,
	).Else(
		txUpdate,
	).Commit()

	if err != nil {
		log.Fatalf("failed to set key: %q", err)
	}

	// jeez
	result := txn.OpResponse().Txn().Responses[0].GetResponseTxn().GetResponses()[1].GetResponseRange().Kvs[0].Value
	if len(res.Kvs) > 0 && res.Kvs[0] != nil {
		copy(data, result)
		val = uint64(binary.LittleEndian.Uint64(data))
	}

	log.Printf("dst val: '%d'", val)
}

func release(ctx context.Context, cli *etcdv3.Client) {
	path := "lock"

	res, err := cli.Get(ctx, path)
	if err != nil {
		log.Fatalf("failed to get key: %q", err)
	}

	var val uint64 = 0
	var data = make([]byte, 8)

	if len(res.Kvs) > 0 && res.Kvs[0] != nil {
		copy(data, res.Kvs[0].Value)
		val = uint64(binary.LittleEndian.Uint64(data))
		log.Printf("src val: '%d'", val)
		if val > 0 {
			val = val - 1
		}
		binary.LittleEndian.PutUint64(data, val)
	} else {
		log.Println("no value for key")
	}

	modRevision := res.Header.GetRevision()

	txCreate := etcdv3.OpTxn(
		nil,
		[]etcdv3.Op{
			etcdv3.OpPut(path, string(data)),
			etcdv3.OpGet(path),
		},
		nil,
	)

	txUpdate := etcdv3.OpTxn(
		[]etcdv3.Cmp{etcdv3.Compare(etcdv3.ModRevision(path), "=", modRevision)},
		[]etcdv3.Op{
			etcdv3.OpPut(path, string(data)),
			etcdv3.OpGet(path),
		},
		nil,
	)

	txnCtx, txnCancel := context.WithTimeout(ctx, time.Second*5)
	defer txnCancel()
	txn, err := cli.Txn(txnCtx).If(
		etcdv3.Compare(etcdv3.CreateRevision(path), "=", 0),
	).Then(
		txCreate,
	).Else(
		txUpdate,
	).Commit()

	if err != nil {
		log.Fatalf("failed to set key: %q", err)
	}

	// jeez
	result := txn.OpResponse().Txn().Responses[0].GetResponseTxn().GetResponses()[1].GetResponseRange().Kvs[0].Value
	if len(res.Kvs) > 0 && res.Kvs[0] != nil {
		copy(data, result)
		val = uint64(binary.LittleEndian.Uint64(data))
	}

	log.Printf("dst val: '%d'", val)
}
