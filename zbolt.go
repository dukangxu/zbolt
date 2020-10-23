package zbolt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"time"
	"unsafe"

	"github.com/boltdb/bolt"
)

// DB database struct, contain boltdb DB struct
type DB struct {
	db *bolt.DB
}

// Tx transaction struct, contain boltdb Tx and error
type Tx struct {
	tx  *bolt.Tx
	err error
}

var (
	_keyPrefix   = []byte{20}
	_valuePrefix = []byte{21}

	_keyMax = Uint64ToBytes(math.MaxUint64)
	_keyMin = Uint64ToBytes(0)
)
var (
	ErrRecordNotFound = errors.New("record not found")
	ErrNil            = errors.New("nil")
)

// Open create DB struct, open file to save db
func Open(path string) (*DB, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 3 * time.Second})
	if err != nil {
		return nil, err
	}
	return &DB{db: db}, nil
}

// NewDB assemble DB struct, input boltdb DB struct
func NewDB(db *bolt.DB) *DB {
	return &DB{db: db}
}

// NewTx create transaction struct
func (db *DB) NewTx(writable bool) *Tx {
	tx := &Tx{}
	tx.tx, tx.err = db.db.Begin(writable)
	return tx
}

//Close close DB
func (db *DB) Close() error {
	return db.db.Close()
}

//Rollback rollback data when some error happened
func (tx *Tx) Rollback() error {
	if tx.tx != nil {
		return tx.tx.Rollback()
	}
	return errors.New("tx nil")
}

//Commit commit data at the end
func (tx *Tx) Commit() error {
	if tx.err == nil {
		return tx.tx.Commit()
	}
	return tx.err
}

//Error set Tx error or return Tx error
func (tx *Tx) Error(errs ...error) error {
	for _, err := range errs {
		if err == ErrNil {
			tx.err = nil
			break
		}
		if err != nil {
			tx.err = err
			break
		}
	}
	return tx.err
}

//createBucketIfWritable create bucket if tx writable and return
func (tx *Tx) createBucketIfWritable(name []byte) *bolt.Bucket {
	var b *bolt.Bucket
	var err error
	if tx.tx.Writable() {
		b, err = tx.tx.CreateBucketIfNotExists(name)
		if tx.Error(err) != nil {
			return nil
		}
		return b
	}
	return tx.tx.Bucket(name)
}

//Get get values from bucket by keys, input multiple and return multiple, like [key1, kye2, ...]
func (tx *Tx) Get(name []byte, keys ...[]byte) [][]byte {
	if tx.err != nil {
		return [][]byte{}
	}
	b := tx.createBucketIfWritable(name)
	if b == nil {
		return [][]byte{}
	}
	var bs [][]byte
	for i := 0; i < len(keys); i++ {
		v := b.Get(keys[i])
		if len(v) != 0 {
			bs = append(bs, keys[i], v)
		}
	}
	return bs
}

// Put put keys values to bucket, input multiple key value, like [key1,value1,key2,value2, ...]
func (tx *Tx) Put(name []byte, kvs ...[]byte) error {
	if tx.err != nil {
		return tx.err
	}
	if len(kvs) == 0 || len(kvs)%2 != 0 {
		return tx.Error(errors.New("key value length must is an even number"))
	}
	b, err := tx.tx.CreateBucketIfNotExists(name)
	if tx.Error(err) != nil {
		return tx.err
	}
	for i := 0; i < len(kvs); i += 2 {
		if tx.Error(b.Put(kvs[i], kvs[i+1])) != nil {
			return tx.err
		}
	}
	return nil
}

// Delete delete value in bucket by keys, input multiple key, like [key1, key2, ...]
func (tx *Tx) Delete(name []byte, keys ...[]byte) error {
	if tx.err != nil {
		return tx.err
	}
	b := tx.tx.Bucket(name)
	if b == nil {
		return nil
	}
	for i := 0; i < len(keys); i += 2 {
		if tx.Error(b.Delete(keys[i])) != nil {
			return tx.err
		}
	}
	return nil
}

//ForEach traveral all key value in bucket
func (tx *Tx) ForEach(name []byte, fn func(k, v []byte) error) error {
	if tx.err != nil {
		return tx.err
	}
	b := tx.tx.Bucket(name)
	if b == nil {
		return nil
	}
	return tx.Error(b.ForEach(fn))
}

//Next get limit count value after key in bucket
func (tx *Tx) Next(name []byte, key []byte, limit int) [][]byte {
	if tx.err != nil {
		return [][]byte{}
	}
	b := tx.createBucketIfWritable(name)
	if b == nil {
		return [][]byte{}
	}
	c := b.Cursor()
	var k, v []byte
	if len(key) == 0 { // if len key == 0, start with first one
		k, v = c.First()
	} else {
		k, v = c.Seek(key)
		if k != nil {
			k, v = c.Next()
		}
	}
	n := 0
	var bs [][]byte
	for k != nil {
		bs = append(bs, k, v)
		n++
		if limit > 0 && n >= limit { //limit = 0 representative of all
			break
		}
		k, v = c.Next()
	}
	return bs
}

// Prev get limit count value front key in bucket
func (tx *Tx) Prev(name []byte, key []byte, limit int) [][]byte {
	if tx.err != nil {
		return [][]byte{}
	}
	b := tx.createBucketIfWritable(name)
	if b == nil {
		return [][]byte{}
	}
	c := b.Cursor()
	var k, v []byte
	if len(key) == 0 { // if len key == 0, start with last one
		k, v = c.Last()
	} else {
		k, v = c.Seek(key)
		if k != nil {
			k, v = c.Prev()
		}
	}
	n := 0
	var bs [][]byte
	for k != nil {
		bs = append(bs, k, v)
		n++
		if limit > 0 && n >= limit { //limit = 0 representative of all
			break
		}
		k, v = c.Prev()
	}
	return bs
}

// Sequence get current sequence in bucket, if bucket not exist, create it, begin with 0
func (tx *Tx) Sequence(name []byte) uint64 {
	if tx.err != nil {
		return 0
	}
	b := tx.createBucketIfWritable(name)
	if b == nil {
		return 0
	}
	return b.Sequence()
}

// NextSequence get next sequence in bucket, if bucket not exist, create it, begin with 0, next 1
func (tx *Tx) NextSequence(name []byte) (uint64, error) {
	if tx.err != nil {
		return 0, tx.err
	}
	b, err := tx.tx.CreateBucketIfNotExists(name)
	if tx.Error(err) != nil {
		return 0, tx.err
	}
	seq, err := b.NextSequence()
	if tx.Error(err) != nil {
		return 0, tx.err
	}
	return seq, nil
}

// DeleteBucket delete bucket
func (tx *Tx) DeleteBucket(name []byte) error {
	if tx.err != nil {
		return tx.err
	}
	return tx.Error(tx.tx.DeleteBucket(name))
}

// SortPut sort put key value to bucket, like timeline as sortKey
func (tx *Tx) SortPut(name []byte, sortKey []byte, kvs ...[]byte) error {
	if tx.err != nil {
		return tx.err
	}
	if len(kvs) == 0 || len(kvs)%2 != 0 {
		return tx.Error(errors.New("key value length must is an even number"))
	}
	keyBucket, err := tx.tx.CreateBucketIfNotExists(BytesConcat(_keyPrefix, name))
	if tx.Error(err) != nil {
		return tx.err
	}
	valueBucket, err := tx.tx.CreateBucketIfNotExists(BytesConcat(_valuePrefix, name))
	if tx.Error(err) != nil {
		return tx.err
	}
	for i := 0; i < len(kvs); i += 2 {
		key, value := kvs[i], kvs[i+1]
		old := valueBucket.Get(key)
		if !bytes.Equal(sortKey, old) {
			if tx.Error(keyBucket.Put(BytesConcat(sortKey, key), value)) != nil {
				return tx.err
			}
			if tx.Error(valueBucket.Put(key, BytesConcat(sortKey, key))) != nil {
				return tx.err
			}
			if old != nil {
				if tx.Error(keyBucket.Delete(old)) != nil {
					return tx.err
				}
			}
		}
	}
	return nil
}

// SortDelete delete key value in bucket with sort
func (tx *Tx) SortDelete(name []byte, keys ...[]byte) error {
	if tx.err != nil {
		return tx.err
	}
	keyBucket, err := tx.tx.CreateBucketIfNotExists(BytesConcat(_keyPrefix, name))
	if tx.Error(err) != nil {
		return tx.err
	}
	valueBucket, err := tx.tx.CreateBucketIfNotExists(BytesConcat(_valuePrefix, name))
	if tx.Error(err) != nil {
		return tx.err
	}
	for i := 0; i < len(keys); i++ {
		value := valueBucket.Get(keys[i])
		if value == nil {
			continue
		}
		if tx.Error(keyBucket.Delete(value)) != nil {
			return tx.err
		}
		if tx.Error(valueBucket.Delete(keys[i])) != nil {
			return tx.err
		}
	}
	return nil
}

// SortDeleteBucket sort delete bucket
func (tx *Tx) SortDeleteBucket(name []byte) error {
	if tx.err != nil {
		return tx.err
	}
	if tx.Error(tx.tx.DeleteBucket(BytesConcat(_keyPrefix, name))) != nil {
		return tx.err
	}
	if tx.Error(tx.tx.DeleteBucket(BytesConcat(_valuePrefix, name))) != nil {
		return tx.err
	}
	return nil
}

// SortNext get limit count key value after key in bucket with sort
func (tx *Tx) SortNext(name []byte, key []byte, limit int) [][]byte {
	if tx.err != nil {
		return [][]byte{}
	}
	b := tx.createBucketIfWritable(BytesConcat(_keyPrefix, name))
	if b == nil {
		return [][]byte{}
	}
	c := b.Cursor()
	var k, v []byte
	if len(key) == 0 { // if len key == 0, start with first one
		k, v = c.First()
	} else {
		k, v = c.Seek(key)
		if k != nil && bytes.Compare(k[:8], key) == 0 {
			k, v = c.Next()
		}
	}
	n := 0
	var bs [][]byte
	for k != nil && bytes.Compare(k[:8], _keyMax) <= 0 {
		bs = append(bs, k[8:], v)
		n++
		if limit > 0 && n >= limit { //limit = 0 representative of all
			break
		}
		k, v = c.Next()
	}
	return bs
}

// SortPrev get limit count key value front key in bucket with sort
func (tx *Tx) SortPrev(name []byte, key []byte, limit int) [][]byte {
	if tx.err != nil {
		return [][]byte{}
	}
	b := tx.createBucketIfWritable(BytesConcat(_keyPrefix, name))
	if b == nil {
		return [][]byte{}
	}
	c := b.Cursor()
	var k, v []byte
	if len(key) == 0 { // if len key == 0, start with last one
		k, v = c.Last()
	} else {
		k, v = c.Seek(key)
		if bytes.Compare(k[:8], key) >= 0 {
			k, v = c.Prev()
		}
	}
	n := 0
	var bs [][]byte
	for k != nil && bytes.Compare(k[:8], _keyMin) >= 0 {
		bs = append(bs, k[8:], v)
		n++
		if limit > 0 && n >= limit { //limit = 0 representative of all
			break
		}
		k, v = c.Prev()
	}
	return bs
}

// BytesConcat concat bytes
func BytesConcat(slices ...[]byte) []byte {
	var totalLen int
	for _, s := range slices {
		totalLen += len(s)
	}
	tmp := make([]byte, totalLen)
	var i int
	for _, s := range slices {
		i += copy(tmp[i:], s)
	}
	return tmp
}

// BucketNameConcat concat bucket name
func BucketNameConcat(slices ...[]byte) []byte {
	var totalLen int
	for _, s := range slices {
		totalLen += len(s)
	}
	underlineNum := len(slices) - 1
	tmp := make([]byte, totalLen+underlineNum)
	var i int
	for j := 0; j < len(slices); j++ {
		if j != 0 {
			i += copy(tmp[i:], []byte("_"))
		}
		i += copy(tmp[i:], slices[j])
	}
	return tmp
}

// Uint64ToBytes parse uint64 to bytes
func Uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// BytesToUint64 parse bytes to unit64
func BytesToUint64(v []byte) uint64 {
	if len(v) == 0 {
		return 0
	}
	return binary.BigEndian.Uint64(v)
}

// BytesToString parse bytes to string
func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// StringToBytes parse string to bytes
func StringToBytes(s string) []byte {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{
		Data: sh.Data,
		Len:  sh.Len,
		Cap:  sh.Len,
	}
	return *(*[]byte)(unsafe.Pointer(&bh))
}
