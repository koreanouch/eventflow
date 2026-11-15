package entity

type OrderStatus string

const (
	StatusPending OrderStatus = "pending"
)

func (s OrderStatus) String() string {
	return string(s)
}
