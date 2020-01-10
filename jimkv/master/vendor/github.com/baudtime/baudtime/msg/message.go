package msg

type Marshaler interface {
	MarshalMsg([]byte) ([]byte, error)
}

type Unmarshaler interface {
	UnmarshalMsg([]byte) ([]byte, error)
}

type Message interface {
	Marshaler
	Unmarshaler
	Msgsize() int
}
