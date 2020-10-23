# zbolt
boltdb wrapper

## install
```bash
go get -u github.com/dukangxu/zbolt
```

## example
```golang
package main

import (
	"github.com/dukangxu/zbolt"
	"fmt"
	"log"
)

func main() {
	db, err := zbolt.Open("z.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	tx := db.NewTx(true)
	defer tx.Rollback()
	bucket := []byte("test")
	//put
	var bs [][]byte
	bs = append(bs, []byte("key1"), []byte("value1"))
	bs = append(bs, []byte("key2"), []byte("value2"))
	tx.Put(bucket, bs...)

	//get
	var keys [][]byte
	keys = append(keys, []byte("key1"))
	keys = append(keys, []byte("key2"))
	gets := tx.Get(bucket, keys...)
	if tx.Error() == nil {
		for i := 0; i < len(gets); i += 2 {
			fmt.Println("key", string(gets[i]), "value", string(gets[i+1]))
		}
	}
	//delete
	keys = [][]byte{[]byte("key1")}
	if err := tx.Delete(bucket, keys...); err != nil {
		log.Fatal(err)
	}

	//get
	keys = append(keys, []byte("key1"))
	keys = append(keys, []byte("key2"))
	gets = tx.Get(bucket, keys...)
	if tx.Error() == nil {
		for i := 0; i < len(gets); i += 2 {
			fmt.Println("key", string(gets[i]), "value", string(gets[i+1]))
		}
	}
	//commit
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
}

output:
key key1 value value1
key key2 value value2
key key2 value value2
```


# acknowledgements
* [boltdb](https://github.com/ego008/youdb)
* [youdb](https://github.com/ego008/youdb)
