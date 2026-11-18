// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"errors"
	"fmt"
	"strings"
)

// Prelude is a sequence of statements that QueryResultContext sends ahead of
// a query, in the query's round trip. Build one with NewPrelude, which
// validates the statements once. A Prelude is immutable, so it may be kept
// and reused across calls, connections and goroutines without being checked
// again. A nil *Prelude is an empty one.
type Prelude struct {
	// text is every statement, each followed by "\n;". Each separator
	// starts a new line, so a trailing -- or # comment cannot swallow what
	// follows it.
	text  string
	count int
	// setsBackslashEscapes reports that the prelude assigns sql_mode a
	// value without NO_BACKSLASH_ESCAPES: in the query after it a backslash
	// escapes in string literals, whatever the mode was before.
	setsBackslashEscapes bool
}

// Len returns the number of statements in p.
func (p *Prelude) Len() int {
	if p == nil {
		return 0
	}
	return p.count
}

// NewPrelude validates statements for QueryResultContext to send ahead of a
// query, and returns them as a Prelude. An error says which statement was
// refused and why; nothing is sent.
//
// The prelude's responses are matched to its statements one result each, so
// each entry must be exactly one statement, without a trailing semicolon,
// that always answers with exactly one result: BEGIN, START TRANSACTION,
// COMMIT, ROLLBACK, SAVEPOINT, RELEASE SAVEPOINT, SET or SELECT. Comments are
// allowed; executable comments (/*! ... */) are code and are refused.
//
// QueryResultContext interpolates the query's args before the server runs the
// prelude, so the prelude must not change how the server reads them:
//
//   - A statement may not contain a backslash. Whether a backslash escapes
//     depends on NO_BACKSLASH_ESCAPES, so where the statement's string
//     literals end would depend on the connection's mode.
//   - A SET that assigns sql_mode must give it a quoted literal or a bare
//     mode name, without NO_BACKSLASH_ESCAPES, so the mode the query is read
//     in is known. DEFAULT, numbers, variables and expressions are refused.
//   - A SET may not change the character set the server reads the query in:
//     SET NAMES, SET CHARACTER SET and character_set_client are refused.
//   - A SET may not assign a GLOBAL or PERSIST variable.
func NewPrelude(statements ...string) (*Prelude, error) {
	p := &Prelude{count: len(statements)}
	var text strings.Builder
	for i, statement := range statements {
		setsBackslashEscapes, err := checkPreludeStatement(statement)
		if err != nil {
			return nil, fmt.Errorf("mysql: prelude statement %d: %w", i, err)
		}
		p.setsBackslashEscapes = p.setsBackslashEscapes || setsBackslashEscapes
		text.WriteString(statement)
		text.WriteString("\n;")
	}
	p.text = text.String()
	return p, nil
}

// checkPreludeStatement validates one prelude statement, reporting whether
// it sets sql_mode to a value in which a backslash escapes.
func checkPreludeStatement(statement string) (setsBackslashEscapes bool, err error) {
	if strings.IndexByte(statement, '\\') >= 0 {
		return false, errors.New("contains a backslash")
	}
	tokens, err := scanPreludeStatement(statement)
	if err != nil {
		return false, err
	}
	if len(tokens) == 0 || tokens[0].kind != tokenWord {
		return false, errors.New("is not a statement the prelude accepts")
	}
	second := func(word string) bool {
		return len(tokens) > 1 && tokens[1].is(word)
	}
	switch strings.ToUpper(tokens[0].text) {
	case "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "SELECT":
		return false, nil
	case "START":
		if second("TRANSACTION") {
			return false, nil
		}
	case "RELEASE":
		if second("SAVEPOINT") {
			return false, nil
		}
	case "SET":
		return checkPreludeSet(tokens[1:])
	}
	return false, fmt.Errorf("%s is not a statement the prelude accepts", tokens[0].text)
}

// checkPreludeSet validates the body of a prelude SET statement.
func checkPreludeSet(tokens []preludeToken) (setsBackslashEscapes bool, err error) {
	if len(tokens) == 0 {
		return false, errors.New("SET assigns nothing")
	}
	switch {
	case tokens[0].is("NAMES"), tokens[0].is("CHARACTER"), tokens[0].is("CHARSET"):
		return false, errors.New("changes the character set the query is read in")
	case tokens[0].is("TRANSACTION"):
		return false, nil
	}
	for _, assignment := range splitTopLevel(tokens) {
		eq := -1
		for i, token := range assignment {
			if token.kind == tokenPunct && (token.text == "=" || token.text == ":=") {
				eq = i
				break
			}
		}
		if eq < 0 {
			return false, errors.New("SET is not a list of variable assignments")
		}
		name, err := preludeSetTarget(assignment[:eq])
		if err != nil {
			return false, err
		}
		switch name {
		case "sql_mode":
			if err := checkPreludeSQLMode(assignment[eq+1:]); err != nil {
				return false, err
			}
			setsBackslashEscapes = true
		case "character_set_client":
			return false, errors.New("changes the character set the query is read in")
		}
	}
	return setsBackslashEscapes, nil
}

// preludeSetTarget returns the lower-case name of the session system
// variable an assignment targets, or "" for a user variable.
func preludeSetTarget(target []preludeToken) (string, error) {
	switch {
	case len(target) == 1 && target[0].kind == tokenUserVar:
		return "", nil
	case len(target) == 1 && target[0].kind == tokenSystemVar:
		scope, name, scoped := strings.Cut(strings.ToLower(target[0].text), ".")
		if !scoped {
			return scope, nil
		}
		if scope != "session" && scope != "local" {
			return "", errors.New("assigns a variable that is not a session variable")
		}
		if name == "" || strings.Contains(name, ".") {
			return "", errors.New("assigns a variable the prelude cannot name")
		}
		return name, nil
	case len(target) == 1 && (target[0].kind == tokenWord || target[0].kind == tokenIdent):
		if target[0].is("GLOBAL") || target[0].is("PERSIST") || target[0].is("PERSIST_ONLY") {
			break
		}
		return strings.ToLower(target[0].text), nil
	case len(target) == 2 && (target[0].is("SESSION") || target[0].is("LOCAL")) &&
		(target[1].kind == tokenWord || target[1].kind == tokenIdent):
		return strings.ToLower(target[1].text), nil
	case len(target) == 2 && (target[0].is("GLOBAL") || target[0].is("PERSIST") || target[0].is("PERSIST_ONLY")):
		return "", errors.New("assigns a variable that is not a session variable")
	}
	return "", errors.New("assigns a variable the prelude cannot name")
}

// checkPreludeSQLMode validates the value assigned to sql_mode: a literal,
// so that the mode the query is read in is known, without
// NO_BACKSLASH_ESCAPES.
func checkPreludeSQLMode(value []preludeToken) error {
	if len(value) != 1 {
		return errors.New("sets sql_mode to a value that is not a literal")
	}
	switch v := value[0]; {
	case v.kind == tokenString:
	case v.kind == tokenWord && !v.is("DEFAULT") && (v.text[0] < '0' || v.text[0] > '9'):
	default:
		return errors.New("sets sql_mode to a value that is not a literal")
	}
	for mode := range strings.SplitSeq(value[0].text, ",") {
		if strings.EqualFold(strings.TrimSpace(mode), "NO_BACKSLASH_ESCAPES") {
			return errors.New("sets sql_mode to include NO_BACKSLASH_ESCAPES")
		}
	}
	return nil
}

// splitTopLevel splits tokens at the commas outside parentheses.
func splitTopLevel(tokens []preludeToken) [][]preludeToken {
	var parts [][]preludeToken
	depth, start := 0, 0
	for i, token := range tokens {
		if token.kind != tokenPunct {
			continue
		}
		switch token.text {
		case "(":
			depth++
		case ")":
			depth--
		case ",":
			if depth == 0 {
				parts = append(parts, tokens[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, tokens[start:])
}

type preludeTokenKind byte

const (
	tokenWord      preludeTokenKind = iota // keyword, unquoted identifier or number
	tokenIdent                             // `quoted identifier`, unquoted
	tokenString                            // 'string' or "string", unquoted
	tokenUserVar                           // @name, without the @
	tokenSystemVar                         // @@[scope.]name, without the @@
	tokenPunct                             // an operator or punctuation character, or :=
)

type preludeToken struct {
	kind preludeTokenKind
	text string
}

func (t preludeToken) is(word string) bool {
	return t.kind == tokenWord && strings.EqualFold(t.text, word)
}

// scanPreludeStatement splits a backslash-free statement into tokens,
// skipping whitespace and comments. With no backslash, every quoted string
// ends at the same place whatever sql_mode is, so the scan does too. It
// refuses a semicolon outside quotes (a second statement), an executable
// comment, and anything unterminated.
func scanPreludeStatement(s string) ([]preludeToken, error) {
	var tokens []preludeToken
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case isPreludeSpace(c):
			i++
		case c == '#' || c == '-' && strings.HasPrefix(s[i:], "--") && (i+2 == len(s) || isPreludeSpace(s[i+2])):
			end := strings.IndexByte(s[i:], '\n')
			if end < 0 {
				i = len(s)
			} else {
				i += end + 1
			}
		case strings.HasPrefix(s[i:], "/*!"):
			return nil, errors.New("contains an executable comment")
		case strings.HasPrefix(s[i:], "/*"):
			end := strings.Index(s[i+2:], "*/")
			if end < 0 {
				return nil, errors.New("contains an unterminated comment")
			}
			i += 2 + end + 2
		case c == '\'' || c == '"' || c == '`':
			// A doubled quote is the quote character itself.
			var text strings.Builder
			j := i + 1
			for {
				end := strings.IndexByte(s[j:], c)
				if end < 0 {
					return nil, errors.New("contains an unterminated quote")
				}
				text.WriteString(s[j : j+end])
				j += end + 1
				if j < len(s) && s[j] == c {
					text.WriteByte(c)
					j++
					continue
				}
				break
			}
			kind := tokenString
			if c == '`' {
				kind = tokenIdent
			}
			tokens = append(tokens, preludeToken{kind: kind, text: text.String()})
			i = j
		case c == ';':
			return nil, errors.New("contains more than one statement")
		case c == '@':
			kind, j := tokenUserVar, i+1
			if j < len(s) && s[j] == '@' {
				kind, j = tokenSystemVar, j+1
			}
			start := j
			for j < len(s) && (isPreludeNameByte(s[j]) || s[j] == '.') {
				j++
			}
			if j == start {
				return nil, errors.New("names a variable the prelude cannot read")
			}
			tokens = append(tokens, preludeToken{kind: kind, text: s[start:j]})
			i = j
		case isPreludeNameByte(c):
			j := i + 1
			for j < len(s) && isPreludeNameByte(s[j]) {
				j++
			}
			tokens = append(tokens, preludeToken{kind: tokenWord, text: s[i:j]})
			i = j
		case c == ':' && strings.HasPrefix(s[i:], ":="):
			tokens = append(tokens, preludeToken{kind: tokenPunct, text: ":="})
			i += 2
		default:
			tokens = append(tokens, preludeToken{kind: tokenPunct, text: s[i : i+1]})
			i++
		}
	}
	return tokens, nil
}

func isPreludeSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\f' || c == '\v'
}

func isPreludeNameByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '$' || c >= 0x80
}

// PreludeError reports that a prelude statement of QueryResultContext
// failed. The server stops at the first statement that fails, so neither the
// rest of the prelude nor the query was executed.
type PreludeError struct {
	// Index is the failed statement's position in the prelude.
	Index int
	// Err is the failure: a *MySQLError for a statement the server rejected.
	Err error
}

func (e *PreludeError) Error() string {
	return fmt.Sprintf("prelude statement %d failed, so the query was not executed: %v", e.Index, e.Err)
}

func (e *PreludeError) Unwrap() error { return e.Err }

// errPreludeIncomplete reports a multi-statement response that ended before
// every prelude statement had answered: the server executed fewer statements
// than were sent.
var errPreludeIncomplete = errors.New("the server's response ended before every prelude statement answered")

// asPreludeError reports err as prelude statement index's failure when it is
// the server rejecting that statement. Any other error — a read failure or a
// cancellation — says nothing about what the server executed, and is
// returned as it is.
func asPreludeError(index int, err error) error {
	var serverErr *MySQLError
	if errors.As(err, &serverErr) {
		return &PreludeError{Index: index, Err: err}
	}
	return err
}
