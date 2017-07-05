package base

import "math/rand"

// 32 legal table name chars:
const inlinetemp = "abcdefghijklmnopqrstuvwxyzabcdef"

func GetSillyName() string { // must be only a-z to fit parser's function definition
	r := rand.Int31()
	s := make([]byte, 10)
	s[0], s[1], s[2], s[3] = 't', 'e', 'm', 'p'
	for a := byte(0); a < 6; a++ {
		s[a+4] = inlinetemp[byte(r>>(a*5))%32]
	}
	return string(s)
}
