package ksqlparser

import (
	"regexp"
	"strings"
)

func isIdentifier(s string) bool {
	for _, rw := range reservedWords {
		if strings.ToUpper(s) == rw {
			return false
		}
	}
	matched, _ := regexp.MatchString("[a-zA-Z_][a-zA-Z_0-9]*", s)
	return matched
}

func isDataType(s string) bool {
	for _, rw := range dataTypes {
		if strings.ToUpper(s) == rw {
			return true
		}
	}
	return false
}

func isIdentifierOrAsterisk(s string) bool {
	return isIdentifier(s) || s == "*"
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func isWhitespaceRune(c rune) bool {
	return c == ' ' || c == '\n' || c == '\r'
}

func isIdentifierRune(c rune) bool {
	return ('a' <= c && c <= 'z') ||
		('A' <= c && c <= 'Z') ||
		('0' <= c && c <= '9') ||
		('_' == c) ||
		('.' == c) ||
		('*' == c)
}

func arrayContains(array []string, contains string) bool {
	for _, i := range array {
		if i == contains {
			return true
		}
	}
	return false
}
