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
	// TODO(ace): handle signals
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// TODO(ace): don't hardcode this.
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
			// TODO(ace): error handling
			acquire(ctx, cli)
			return nil
		},
	}

	var releaseCmd = &cobra.Command{
		Use:   "release",
		Short: "Release removes a holder from a semaphore resource.",
		RunE: func(cmd *cobra.Command, args []string) error {
			// TODO(ace): error handling
			release(ctx, cli)
			return nil
		},
	}
	rootCmd.AddCommand(acquireCmd)
	rootCmd.AddCommand(releaseCmd)
	rootCmd.Execute()
}

func acquire(ctx context.Context, cli *etcdv3.Client) {
	// TODO(ace): parameterize
	path := "lock"

	res, err := cli.Get(ctx, path)
	if err != nil {
		log.Fatalf("failed to get key: %q", err)
	}

	// acquire adds a holder on an existing semaphore,
	// or creates a new semaphore with a single holder.
	var data = make([]byte, 8)
	var val uint64 = 1

	// range request will always succeed but may not have any values.
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

	// ensure revision has not changed for transaction semantics.
	getRevision := res.Header.GetRevision()
	binary.LittleEndian.PutUint64(data, val)

	// create semaphore if not exist and set holder count to 1
	txCreate := etcdv3.OpTxn(
		nil,
		[]etcdv3.Op{
			etcdv3.OpPut(path, string(data)),
			etcdv3.OpGet(path),
		},
		nil,
	)

	// ensure revision matches GET and increment value from GET by 1.
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

	// run create/update txn based on existing of key (== 0 implies not exist.)
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
	// extract the value from the final GET from either the create or the update txn.
	// this will be the new number of semaphore holders for either case.
	result := txn.OpResponse().Txn().Responses[0].GetResponseTxn().GetResponses()[1].GetResponseRange().Kvs[0].Value
	if len(res.Kvs) > 0 && res.Kvs[0] != nil {
		copy(data, result)
		val = uint64(binary.LittleEndian.Uint64(data))
	}

	log.Printf("dst val: '%d'", val)
}

func release(ctx context.Context, cli *etcdv3.Client) {
	// TODO(ace): parameterize
	path := "lock"

	// get the existing semaphore
	// will always succeed if not present, but no values
	res, err := cli.Get(ctx, path)
	if err != nil {
		log.Fatalf("failed to get key: %q", err)
	}

	// releasing the semaphore decrements existing count by 1
	// or creates and sets to zero if not exist.
	var val uint64 = 0
	var data = make([]byte, 8)

	// previous value was found, use it
	if len(res.Kvs) > 0 && res.Kvs[0] != nil {
		copy(data, res.Kvs[0].Value)
		val = uint64(binary.LittleEndian.Uint64(data))
		log.Printf("src val: '%d'", val)
		// avoid underflow
		// better way?
		if val > 0 {
			val = val - 1
		}
		binary.LittleEndian.PutUint64(data, val)
	} else {
		log.Println("no value for key")
	}

	// use revision from GET for optimistic concurrency control
	modRevision := res.Header.GetRevision()

	// if value not exist, create with 0 holders.
	txCreate := etcdv3.OpTxn(
		nil,
		[]etcdv3.Op{
			etcdv3.OpPut(path, string(data)),
			etcdv3.OpGet(path),
		},
		nil,
	)

	// remove one holder to the semaphore assuming modRevision has not changed.
	txUpdate := etcdv3.OpTxn(
		[]etcdv3.Cmp{etcdv3.Compare(etcdv3.ModRevision(path), "=", modRevision)},
		[]etcdv3.Op{
			etcdv3.OpPut(path, string(data)),
			etcdv3.OpGet(path),
		},
		nil,
	)

	// if exists, decrement holders by 1, or else create with 0 holders.
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
	// extract the value from the final GET from either the create or the update txn.
	// this will be the new number of semaphore holders for either case.
	result := txn.OpResponse().Txn().Responses[0].GetResponseTxn().GetResponses()[1].GetResponseRange().Kvs[0].Value
	if len(res.Kvs) > 0 && res.Kvs[0] != nil {
		copy(data, result)
		val = uint64(binary.LittleEndian.Uint64(data))
	}

	log.Printf("dst val: '%d'", val)
}
