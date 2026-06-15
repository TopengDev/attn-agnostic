package agent

import (
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// emojiMap mirrors the upstream EMOJI_MAP shortcodes.
var emojiMap = map[string]string{
	"thumbs_up": "👍", "thumbs_down": "👎", "heart": "❤️", "fire": "🔥",
	"check": "✅", "x": "❌", "star": "⭐", "eyes": "👀", "rocket": "🚀",
	"party": "🎉", "wave": "👋", "clap": "👏", "laugh": "😂", "think": "🤔",
	"hundred": "💯", "pray": "🙏",
}

func emojiToUnicode(in string) string {
	if v, ok := emojiMap[in]; ok {
		return v
	}
	return in
}

var durationRe = regexp.MustCompile(`^(\d+)\s*(m|h|d|w)$`)

// parseDuration parses "30m", "1h", "1d", "7d", "2w".
func parseDuration(in string) (time.Duration, bool) {
	m := durationRe.FindStringSubmatch(strings.ToLower(strings.TrimSpace(in)))
	if m == nil {
		return 0, false
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, false
	}
	var unit time.Duration
	switch m[2] {
	case "m":
		unit = time.Minute
	case "h":
		unit = time.Hour
	case "d":
		unit = 24 * time.Hour
	case "w":
		unit = 7 * 24 * time.Hour
	}
	return time.Duration(n) * unit, true
}

func formatRemaining(ms int64) string {
	if ms < 60_000 {
		return fmt.Sprintf("%ds", (ms+999)/1000)
	}
	if ms < 3_600_000 {
		return fmt.Sprintf("%dm", (ms+59_999)/60_000)
	}
	if ms < 86_400_000 {
		return fmt.Sprintf("%dh", (ms+3_599_999)/3_600_000)
	}
	return fmt.Sprintf("%dd", (ms+86_399_999)/86_400_000)
}

func isGlobalMuteTarget(in string) bool {
	t := strings.ToLower(strings.TrimSpace(in))
	return t == "all" || t == "*" || t == "everyone"
}

func durSuffix(untilMs int64) string {
	if untilMs <= 0 {
		return " indefinitely"
	}
	return " for " + formatRemaining(untilMs-time.Now().UnixMilli())
}

func arrivedSuffix(count int) string {
	if count <= 0 {
		return ""
	}
	plural := "s"
	if count == 1 {
		plural = ""
	}
	return fmt.Sprintf(" — %d message%s arrived while muted (use history to read)", count, plural)
}

// weiToEth renders a wei amount as a decimal ETH string (best-effort, trims).
func weiToEth(wei *big.Int) string {
	if wei == nil {
		return "0"
	}
	ether := new(big.Rat).SetFrac(wei, big.NewInt(1_000_000_000_000_000_000))
	s := strings.TrimRight(strings.TrimRight(ether.FloatString(6), "0"), ".")
	if s == "" {
		return "0"
	}
	return s
}

func simErrString(err error) string {
	if err == nil {
		return "ok (eth_call would succeed)"
	}
	return "would revert: " + err.Error()
}

func datePart(iso string) string {
	if i := strings.IndexByte(iso, 'T'); i >= 0 {
		return iso[:i]
	}
	return iso
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func intArg(args map[string]any, key string, def int) int {
	v, ok := args[key]
	if !ok {
		return def
	}
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	case string:
		if i, err := strconv.Atoi(n); err == nil {
			return i
		}
	}
	return def
}

func boolArg(args map[string]any, key string) bool {
	if v, ok := args[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}

func strSlice(v any) []string {
	switch arr := v.(type) {
	case []string:
		return arr
	case []any:
		out := make([]string, 0, len(arr))
		for _, e := range arr {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}
