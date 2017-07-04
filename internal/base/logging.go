package base

// Debug using fmt.Println. Call nodb.EnableLogging()
var Debug = func(i ...interface{}) (int, error) { return 0, nil }
