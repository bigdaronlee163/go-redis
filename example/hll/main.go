package main

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:        ":6379",
		ReadTimeout: -1,  // 这个参数是怎么配置的。 
	})
	// _ = rdb.FlushDB(ctx).Err()

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

	// card, err := rdb.SCard(ctx, "myset").Result()
	// if err != nil {
	// 	panic(err)
	// }

	// fmt.Println("set cardinality", card)

	mems, err := rdb.SMembers(ctx, "myset").Result()
	if err != nil {
		panic(err)
	}

	fmt.Println("set mems: ", mems)
}
