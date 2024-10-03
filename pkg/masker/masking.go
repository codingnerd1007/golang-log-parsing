package masker

import (
	"fmt"
	"go.elara.ws/pcre"
)

// MaskRule holds the pre-compiled regex pattern and the corresponding mask
type MaskRule struct {
	Regex    *pcre.Regexp
	MaskWith string
}

// CompileMaskRules compiles the regex patterns once for better performance
func CompileMaskRules(rules map[string]string) ([]MaskRule, error) {
	compiledRules := make([]MaskRule, 0, len(rules))
	for pattern, mask := range rules {
		re, err := pcre.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("failed to compile regex: %v", err)
		}
		compiledRules = append(compiledRules, MaskRule{Regex: re, MaskWith: mask})
	}
	return compiledRules, nil
}

// ApplyMask applies the pre-compiled mask rules to a log message
func ApplyMask(logMessage string, rules []MaskRule) string {
	for _, rule := range rules {
		logMessage = rule.Regex.ReplaceAllString(logMessage, rule.MaskWith)
	}
	return logMessage
}
