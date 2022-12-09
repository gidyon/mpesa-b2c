package b2c_app_v1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gidyon/mpesab2c/pkg/utils/httputils"
)

func (b2cAPI *b2cAPIServer) updateAccessTokenWorker(ctx context.Context, dur time.Duration) {
	var (
		err         error
		sleep       = time.Second * 10
		ticker      = time.NewTicker(dur)
		updateToken func()
	)

	updateToken = func() {
		err = b2cAPI.updateAccessToken()
		if err != nil {
			b2cAPI.Logger.Errorf("failed to update access token: %v", err)
			time.Sleep(sleep)
			sleep = sleep * 2
		} else {
			b2cAPI.Logger.Infoln("access token updated")
			ticker.Reset(dur)
			sleep = time.Second * 10
		}
	}

	updateToken()

	ticker.Reset(dur)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			updateToken()
		}
	}
}

func (b2cAPI *b2cAPIServer) updateAccessToken() error {
	req, err := http.NewRequest(http.MethodGet, b2cAPI.B2COptions.AccessTokenURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Basic %s", b2cAPI.B2COptions.basicToken))

	httputils.DumpRequest(req, "B2C ACCESS TOKEN REQUEST")

	res, err := b2cAPI.HTTPClient.Do(req)
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("request failed: %v", err)
	}

	httputils.DumpResponse(res, "B2C ACCESS TOKEN RESPONSE")

	switch {
	case res.StatusCode != http.StatusOK:
		return fmt.Errorf("expected status ok got: %v", res.StatusCode)
	case !strings.Contains(strings.ToLower(res.Header.Get("content-type")), "application/json"):
		return fmt.Errorf("expected application/json got: %v", res.Header.Get("content-type"))
	}

	resTo := make(map[string]interface{})

	err = json.NewDecoder(res.Body).Decode(&resTo)
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("failed to json decode response: %v", err)
	}

	b2cAPI.B2COptions.accessToken = fmt.Sprint(resTo["access_token"])

	return nil
}
