package rediscluster

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

func main() {
	// Background 的Done直接是返回 nil 的。
	ctx := context.Background()

	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{":3006", ":3001", ":3002", ":3003", ":3004", ":3005"},
	})
	mems, err := rdb.SMembers(ctx, "myset").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("set mems: ", mems)

}
