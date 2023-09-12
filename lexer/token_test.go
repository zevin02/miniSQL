package lexer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestToken(t *testing.T) {
	realToken := NewToken(REAL)
	assert.Equal(t, "REAL", realToken.ToString())
	idToken := NewToken(ID)
	assert.Equal(t, "ID", idToken.ToString())
	
}
