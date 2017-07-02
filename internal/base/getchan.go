package base

// GetChanError is the GetChan return type
type GetChanError struct {
	Item []interface{}
	Err  error
}

// Obj conveys "table data" as Do's 3rd arg
type Obj map[string]interface{}
