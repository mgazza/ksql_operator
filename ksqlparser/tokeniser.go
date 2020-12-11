package ksqlparser

import (
	"fmt"
	"strings"
)

func (p *parser) peek(reservedWords ...string) string {
	peeked, _ := p.peekWithLength(reservedWords...)
	return peeked
}

func (p *parser) pop(reservedWords ...string) string {
	peeked, l := p.peekWithLength(reservedWords...)
	p.i += l
	p.popWhitespace()
	return peeked
}

func (p *parser) popOrError(reservedWords ...string) (string, error) {
	peeked, l, err := p.peekWithLengthOrError(reservedWords...)
	if err != nil {
		return "", err
	}
	p.i += l
	p.popWhitespace()
	return peeked, nil
}

func (p *parser) popLength(len int) {
	p.i += len
	p.popWhitespace()
}

func (p *parser) popWhitespace() {
	for ; p.i < len(p.sql) && isWhitespaceRune(rune(p.sql[p.i])); p.i++ {
		if p.sql[p.i] == '\n' {
			p.line++
			p.col = -1
		}
		p.col++
	}
	// check for multiline comment start
	if "/*" == p.sql[p.i:min(len(p.sql), p.i+2)] {
		p.col += 2
		for p.i += 2; p.i < len(p.sql); p.i++ {
			if "*/" == strings.ToUpper(p.sql[p.i:min(len(p.sql), p.i+2)]) {
				p.col += 2
				p.i += 2
				break
			}
			if p.sql[p.i] == '\n' {
				p.line++
				p.col = -1
			}
			p.col++
		}
		//ensure that were out of whitespace
		p.popWhitespace()
	}
	// check for singleline comment
	if "--" == p.sql[p.i:min(len(p.sql), p.i+2)] {
		p.col += 2
		for p.i += 2; p.i < len(p.sql); p.i++ {
			if p.sql[p.i] == '\n' {
				p.line++
				p.col = 0
				break
			}
			p.col++
		}
		p.popWhitespace()
	}
}

func (p *parser) peekWithLengthOrError(reservedWords ...string) (string, int, error) {
	if p.i >= len(p.sql) {
		return "", 0, p.Error(fmt.Sprintf("[%s]", strings.Join(reservedWords, ", ")))
	}
	for _, rWord := range reservedWords {
		token := strings.ToUpper(p.sql[p.i:min(len(p.sql), p.i+len(rWord))])
		if token == rWord {
			return token, len(token), nil
		}
	}
	return "", 0, p.Error(fmt.Sprintf("[%s]", strings.Join(reservedWords, ", ")))
}

func (p *parser) peekWithLength(reservedWords ...string) (string, int) {
	if p.i >= len(p.sql) {
		return "", 0
	}
	for _, rWord := range reservedWords {
		token := strings.ToUpper(p.sql[p.i:min(len(p.sql), p.i+len(rWord))])
		if token == rWord {
			return token, len(token)
		}
	}
	if p.sql[p.i] == '\'' { // Quoted string
		return p.peekQuotedStringWithLength()
	}
	return p.peekIdentifierWithLength()
}

func (p *parser) peekQuotedStringWithLength() (string, int) {
	if len(p.sql) < p.i || p.sql[p.i] != '\'' {
		return "", 0
	}
	for i := p.i + 1; i < len(p.sql); i++ {
		if p.sql[i] == '\'' && p.sql[i-1] != '\\' {
			return p.sql[p.i : i+1], (i + 1) - p.i
		}
	}
	return "", 0
}

func (p *parser) peekIdentifierWithLength() (string, int) {
	for i := p.i; i < len(p.sql); i++ {
		snip := p.sql[i:min(len(p.sql), i+2)]
		if "->" == snip {
			i++      //progress past the -
			continue //continue will progress the >
		}
		if !isIdentifierRune(rune(p.sql[i])) {
			return p.sql[p.i:i], len(p.sql[p.i:i])
		}
	}
	return p.sql[p.i:], len(p.sql[p.i:])
}
