package internal

type (
	Key   int
	Value int
)

type SetCommand struct {
	K Key
	V Value
}

type GetCommand struct {
	K Key
}

type ExistsCommand struct {
	K Key
}

type DelCommand struct {
	K Key
}

type RangeCommand struct {
	From Key
	To   Key
}
