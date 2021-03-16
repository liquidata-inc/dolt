// Copyright 2020 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commands

import (
	"bufio"
	"io"
	"unicode"
)

type statementScanner struct {
	*bufio.Scanner
	statementStartLine int // the line number of the first line of the last parsed statement
	startLineNum       int // the line number we began parsing the most recent token at
	lineNum            int // the current line number being parsed
	Delimiter          string
}

const maxStatementBufferBytes = 100 * 1024 * 1024

func NewSqlStatementScanner(input io.Reader) *statementScanner {
	scanner := bufio.NewScanner(input)
	const initialCapacity = 512 * 1024
	buf := make([]byte, initialCapacity)
	scanner.Buffer(buf, maxStatementBufferBytes)

	s := &statementScanner{
		Scanner:   scanner,
		lineNum:   1,
		Delimiter: ";",
	}
	scanner.Split(s.scanStatements)

	return s
}

const (
	sQuote    byte = '\''
	dQuote         = '"'
	backslash      = '\\'
	backtick       = '`'
)

// ScanStatements is a split function for a Scanner that returns each SQL statement in the input as a token.
func (s *statementScanner) scanStatements(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	var (
		quoteChar                      byte // the opening quote character of the current quote being parsed, or 0 if the current parse location isn't inside a quoted string
		lastChar                       byte // the last character parsed
		ignoreNextChar                 bool // whether to ignore the next character
		numConsecutiveBackslashes      int  // the number of consecutive backslashes encountered
		seenNonWhitespaceChar          bool // whether we have encountered a non-whitespace character since we returned the last token
		numConsecutiveDelimiterMatches int  // the consecutive number of characters that have been matched to the delimiter
	)

	s.startLineNum = s.lineNum

	for i := 0; i < len(data); i++ {
		if !ignoreNextChar {
			// this doesn't handle unicode characters correctly and will break on some things, but it's only used for line
			// number reporting.
			if !seenNonWhitespaceChar && !unicode.IsSpace(rune(data[i])) {
				seenNonWhitespaceChar = true
				s.statementStartLine = s.lineNum
			}
			// check if we've matched the delimiter string
			if quoteChar == 0 && data[i] == s.Delimiter[numConsecutiveDelimiterMatches] {
				numConsecutiveDelimiterMatches++
				if numConsecutiveDelimiterMatches == len(s.Delimiter) {
					s.startLineNum = s.lineNum
					_, _, _ = s.resetState()
					removalLength := len(s.Delimiter) - 1 // We remove the delimiter so it depends on the length
					return i + 1, data[0 : i-removalLength], nil
				}
				lastChar = data[i]
				continue
			} else {
				numConsecutiveDelimiterMatches = 0
			}

			switch data[i] {
			case '\n':
				s.lineNum++
			case backslash:
				numConsecutiveBackslashes++
			case sQuote, dQuote, backtick:
				prevNumConsecutiveBackslashes := numConsecutiveBackslashes
				numConsecutiveBackslashes = 0

				// escaped quote character
				if lastChar == backslash && prevNumConsecutiveBackslashes%2 == 1 {
					break
				}

				// currently in a quoted string
				if quoteChar != 0 {

					// end quote or two consecutive quote characters (a form of escaping quote chars)
					if quoteChar == data[i] {
						var nextChar byte = 0
						if i+1 < len(data) {
							nextChar = data[i+1]
						}

						if nextChar == quoteChar {
							// escaped quote. skip the next character
							ignoreNextChar = true
							break
						} else if atEOF || i+1 < len(data) {
							// end quote
							quoteChar = 0
							break
						} else {
							// need more data to make a decision
							return s.resetState()
						}
					}

					// embedded quote ('"' or "'")
					break
				}

				// open quote
				quoteChar = data[i]
			default:
				numConsecutiveBackslashes = 0
			}
		} else {
			ignoreNextChar = false
		}

		lastChar = data[i]
	}

	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), data, nil
	}

	// Request more data.
	return s.resetState()
}

// resetState resets the internal state of the scanner and returns the "more data" response for a split function
func (s *statementScanner) resetState() (advance int, token []byte, err error) {
	// rewind the line number to where we started parsing this token
	s.lineNum = s.startLineNum
	return 0, nil, nil
}
