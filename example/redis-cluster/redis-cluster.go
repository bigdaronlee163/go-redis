package main

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

func main() {
	// Background 的Done直接是返回 nil 的。
	ctx := context.Background()

	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{":30006", ":30001", ":30002", ":30003", ":30004", ":30005"},
	})

	// for i := 0; i < 1; i++ {
	if err := rdb.SAdd(ctx, "myset", fmt.Sprint(1)).Err(); err != nil {
		panic(err)
	}

	if err := rdb.SAdd(ctx, "myset", fmt.Sprint(2)).Err(); err != nil {
		panic(err)
	}

	if err := rdb.SAdd(ctx, "myset", fmt.Sprint(3)).Err(); err != nil {
		panic(err)
	}
	// }

	card, err := rdb.SCard(ctx, "myset").Result()
	if err != nil {
		panic(err)
	}

	fmt.Println("set card: ", card)

	mems, err := rdb.SMembers(ctx, "myset").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("set mems: ", mems)

}
