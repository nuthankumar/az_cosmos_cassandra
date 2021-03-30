package utils

import (
	"fmt"
	"strings"
	"time"
	"unicode"
)

func LogInfo(message string) {
	currentTime := time.Now()
	fmt.Println(currentTime.Format("2006-01-02 3:4:5"), "INFO : ", message)
}
func LogError(message string) {
	currentTime := time.Now()
	fmt.Println(currentTime.Format("2006-01-02 3:4:5"), "ERROR : ", message)
}
func LogWarn(message string) {
	currentTime := time.Now()
	fmt.Println(currentTime.Format("2006-01-02 3:4:5"), "WARNING : ", message)
}

func StripSpaces(str string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, str)
}

func contains(slice []string, item string) bool {
	set := make(map[string]struct{}, len(slice))
	for _, s := range slice {
		set[s] = struct{}{}
	}

	_, ok := set[item]
	return ok
}
