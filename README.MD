# GO CSV GZip Writer

## Reference

> [`github.com/jszwec/csvutil`](https://github.com/jszwec/csvutil)

```golang
package main

import (
	"fmt"
	"os"

	csvgz "github.com/attapon-th/go-csvgzwriter"
)

type User struct {
	ID   int 	`csv:"id"`
	Name string `csv:"name"`
	Age  int 	`csv:",omitempty"`
	City string	`csv:"city"`
}

func main() {
	f, _ := os.OpenFile("test.csv.gz", os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_TRUNC, 0660)
	c, _ := csvgz.New(f)
	defer c.Close()
	var users = []User{}
	users = append(users, User{})
	///  add data to Struct User
	_ = c.MarshalStuctSlice(users)
	fmt.Println("Totle CSV Record: ", c.TotalRows)

}

```
