package main

import (
	api "./sphinxapi"
	"fmt"
	"encoding/json"
	"time"
	//"github.com/pkg/profile"
)

func main() {
	//defer profile.Start(profile.MemProfile, profile.ProfilePath(".")).Stop()

	cb := func(err error, result interface{}) {
		if err != nil {
			fmt.Println("ERROR", err)
		} else {
			//fmt.Println("CALLBACK", result)
			buf, _ := json.MarshalIndent(result, "", "   ")
			fmt.Println(string(buf))
		}
	}

	cl := api.CreateSphinxClient()
	//fmt.Println("CLIENT", cl)

	//cl.Status(cb)
	w := map[string]int{
		"title": 0x64,
		"content": 1,
	}
	cl.SetFieldWeights(w)

	now := time.Now()
	cl.Open(cb)
	cl.Query("test", "rt", "", cb)
	cl.Query("test2", "rt", "", cb)
	cl.Query("test2", "rt", "", cb)
	cl.Query("test3", "rt", "", cb)
	fmt.Println(time.Since(now))
}
