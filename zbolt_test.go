package zbolt

import (
	"fmt"
	"testing"
)

var db *DB
var err error
var bucket = []byte("test")

func init() {
	db, err = Open("z.db")
	if err != nil {
		panic(err)
	}
}

func TestTx_Error(t *testing.T) {
	tx := db.NewTx(false)
	defer tx.Rollback()
	fmt.Println(tx.Error(ErrRecordNotFound))
	fmt.Println(tx.Error())
	fmt.Println(tx.Error(ErrNil))
	fmt.Println(tx.Error())
}

func TestTx_Put(t *testing.T) {
	tx := db.NewTx(true)
	defer tx.Rollback()
	var bs [][]byte
	bs = append(bs, []byte("key1"), []byte("value1"))
	bs = append(bs, []byte("key2"), []byte("value2"))
	bs = append(bs, []byte("key3"), []byte("value3"))
	bs = append(bs, []byte("key4"), []byte("value4"))
	bs = append(bs, []byte("key5"), []byte("value5"))
	tx.Put(bucket, bs...)
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkTx_Put(b *testing.B) {
	tx := db.NewTx(true)
	defer tx.Rollback()
	bucket := []byte("test")
	for i := 0; i < b.N; i++ {
		bs := [][]byte{[]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i))}
		tx.Put(bucket, bs...)
	}
	tx.Commit()
}

func TestTx_Get(t *testing.T) {
	tx := db.NewTx(false)
	defer tx.Rollback()
	var keys [][]byte
	keys = append(keys, []byte("key1"))
	keys = append(keys, []byte("key2"))
	keys = append(keys, []byte("key3"))
	keys = append(keys, []byte("key4"))
	keys = append(keys, []byte("key5"))
	gets := tx.Get(bucket, keys...)
	for i := 0; i < len(gets); i += 2 {
		fmt.Println("key", string(gets[i]), "value", string(gets[i+1]))
	}
}

func BenchmarkTx_Get(b *testing.B) {
	tx := db.NewTx(false)
	defer tx.Rollback()
	bucket := []byte("test")
	for i := 0; i < b.N; i++ {
		bs := [][]byte{[]byte(fmt.Sprintf("key%d", i))}
		tx.Get(bucket, bs...)
	}
}

func TestTx_ForEach(t *testing.T) {
	tx := db.NewTx(false)
	defer tx.Rollback()
	tx.ForEach(bucket, func(k, v []byte) error {
		fmt.Println(string(k), string(v))
		return nil
	})
}

func TestTx_Next(t *testing.T) {
	tx := db.NewTx(false)
	defer tx.Rollback()
	fmt.Println("========get key3 next==========")
	gets := tx.Next(bucket, []byte("key3"), 10)
	for i := 0; i < len(gets); i += 2 {
		fmt.Println("key", string(gets[i]), "value", string(gets[i+1]))
	}

	fmt.Println("========get all next==========")
	gets = tx.Next(bucket, nil, 10)
	for i := 0; i < len(gets); i += 2 {
		fmt.Println("key", string(gets[i]), "value", string(gets[i+1]))
	}
}

func TestTx_Prev(t *testing.T) {
	tx := db.NewTx(false)
	defer tx.Rollback()
	fmt.Println("========get key3 next==========")
	gets := tx.Prev(bucket, []byte("key3"), 10)
	for i := 0; i < len(gets); i += 2 {
		fmt.Println("key", string(gets[i]), "value", string(gets[i+1]))
	}

	fmt.Println("========get all next==========")
	gets = tx.Prev(bucket, nil, 10)
	for i := 0; i < len(gets); i += 2 {
		fmt.Println("key", string(gets[i]), "value", string(gets[i+1]))
	}
}

func TestTx_SortPut(t *testing.T) {
	tx := db.NewTx(true)
	defer tx.Rollback()

	tx.SortPut(bucket, Uint64ToBytes(1), Uint64ToBytes(1), []byte("1"))
	tx.SortPut(bucket, Uint64ToBytes(3), Uint64ToBytes(3), []byte("3"))
	tx.SortPut(bucket, Uint64ToBytes(5), Uint64ToBytes(5), []byte("5"))
	tx.SortPut(bucket, Uint64ToBytes(7), Uint64ToBytes(5), []byte("7"))
	tx.SortPut(bucket, Uint64ToBytes(9), Uint64ToBytes(5), []byte("9"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestTx_SortNext(t *testing.T) {
	tx := db.NewTx(false)
	defer tx.Rollback()

	next := tx.SortNext(bucket, Uint64ToBytes(0), 10)
	if tx.Error() != nil {
		t.Fatal(tx.Error())
	}
	for i := 0; i < len(next); i += 2 {
		fmt.Println(BytesToUint64(next[i]), string(next[i+1]))
	}
}

func TestTx_SortPrev(t *testing.T) {
	tx := db.NewTx(false)
	defer tx.Rollback()
	prev := tx.SortPrev(bucket, Uint64ToBytes(4), 2)
	if tx.Error() != nil {
		t.Fatal(tx.Error())
	}
	for i := 0; i < len(prev); i += 2 {
		fmt.Println(BytesToUint64(prev[i]))
	}
}

func TestTx_SortDelete(t *testing.T) {
	tx := db.NewTx(true)
	tx.SortPut(bucket, Uint64ToBytes(1), Uint64ToBytes(1), []byte("1"))
	tx.SortPut(bucket, Uint64ToBytes(3), Uint64ToBytes(3), []byte("3"))
	tx.SortPut(bucket, Uint64ToBytes(5), Uint64ToBytes(5), []byte("5"))
	tx.SortPut(bucket, Uint64ToBytes(7), Uint64ToBytes(7), []byte("7"))
	tx.SortPut(bucket, Uint64ToBytes(9), Uint64ToBytes(5), []byte("9"))
	if err := tx.Commit(); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	tx.Rollback()
	fmt.Println("======================")
	//------------------------------------------------------------
	tx1 := db.NewTx(false)
	prev := tx1.SortNext(bucket, nil, 10)
	if tx1.Error() != nil {
		t.Fatal(tx1.Error())
	}
	for i := 0; i < len(prev); i += 2 {
		fmt.Println(BytesToUint64(prev[i]))
	}
	tx1.Rollback()
	fmt.Println("======================")
	//------------------------------------------------------------
	tx2 := db.NewTx(true)
	tx2.SortDelete(bucket, Uint64ToBytes(1))
	if err := tx2.Commit(); err != nil {
		tx2.Rollback()
		t.Fatal(err)
	}
	tx2.Rollback()
	fmt.Println("======================")
	//------------------------------------------------------------
	tx3 := db.NewTx(false)
	prev3 := tx3.SortNext(bucket, nil, 10)
	if tx3.Error() != nil {
		t.Fatal(tx3.Error())
	}
	for i := 0; i < len(prev3); i += 2 {
		fmt.Println(BytesToUint64(prev3[i]))
	}
	tx1.Rollback()
	fmt.Println("======================")
}
