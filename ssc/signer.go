package ssc

type BLSSigner interface {
	Sign([]byte) ([]byte, error)
	Aggregate([][]byte) ([]byte, error)
}
