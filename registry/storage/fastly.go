// Temporary home for Fastly Code

package storage

import (
	"crypto/hmac"
	"crypto/sha1"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"
)

// TODO: from config
const fastlyHost = "cdn.docker.com.global.prod.fastly.net"

// fastlyURL converts an S3 URL into a fastly URL
// with a TTL token valud until expires
func fastlyURL(s3URL string, expires time.Time) (string, error) {
	u, err := url.Parse(s3URL)
	if err != nil {
		return "", err
	}

	pathWithoutBucket := strings.Split(u.Path, "/")[2:]
	path := "/" + strings.Join(pathWithoutBucket, "/")

	key := os.Getenv("FASTLYKEY")
	if key == "" {
		return "", fmt.Errorf("missing FASTLY-KEY env variable")
	}
	fastlyToken := makeFastlyToken(path, key, expires)
	if err != nil {
		return "", err
	}
	u.Host = fastlyHost
	u.Path = path
	q := u.Query()
	q.Add("token", fastlyToken)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func makeFastlyToken(path, key string, expiration time.Time) string {
	keyBytes := []byte(key)
	expirationHex := fmt.Sprintf("%x", expiration.Unix())
	toSign := fmt.Sprintf("%s%s", path, expirationHex)
	mac := hmac.New(sha1.New, keyBytes)
	mac.Write([]byte(toSign))
	signature := fmt.Sprintf("0x%x", mac.Sum(nil))
	token := fmt.Sprintf("%s_%s", expirationHex, signature)
	return token
}
