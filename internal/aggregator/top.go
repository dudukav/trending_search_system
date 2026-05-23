package aggregator

type TopItem struct {
	Query 	string	`json:"query"`
	Count 	int64	`json:"count"`
	Rank 	int		`json:"rank"`
}