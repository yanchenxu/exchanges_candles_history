package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/nntaoli-project/goex"
)

type Bitfinex struct {
	client               *http.Client
	accessKey, secretKey string
}

const (
	BASE_URL = "https://api-pub.bitfinex.com/v2/"
)

func NewBitfinex(client *http.Client, accessKey, secretKey string) *Bitfinex {
	return &Bitfinex{client, accessKey, secretKey}
}

func (bfx *Bitfinex) GetExchangeName() string {
	return goex.BITFINEX
}

func (bfx *Bitfinex) GetKlineRecords(currencyPair goex.CurrencyPair, period, size, since, end int) ([]goex.Kline, error) {
	// https://api-pub.bitfinex.com/v2/candles/trade:1m:tBTCUSD/hist?limit=100&start=1514779200000&end=1514782800000&sort=-1

	URL := BASE_URL + "candles/trade:%s:%s/hist?limit=%d&start=%d&end=%d&sort=1"

	granularity := "1m"
	switch period {
	case goex.KLINE_PERIOD_1MIN:
		granularity = "1m"
	case goex.KLINE_PERIOD_5MIN:
		granularity = "5m"
	case goex.KLINE_PERIOD_15MIN:
		granularity = "15m"
	case goex.KLINE_PERIOD_30MIN:
		granularity = "30m"
	case goex.KLINE_PERIOD_1H:
		granularity = "1h"
	case goex.KLINE_PERIOD_6H:
		granularity = "6h"
	case goex.KLINE_PERIOD_12H:
		granularity = "12h"
	case goex.KLINE_PERIOD_1DAY:
		granularity = "1D"
	case goex.KLINE_PERIOD_1WEEK:
		granularity = "7D"
	default:
		return nil, fmt.Errorf("unsupport period %d ", period)
	}

	pair := "t" + currencyPair.CurrencyA.String() + "USD"

	if size > 10000 {
		size = 10000
	}

	respData, err := NewHttpRequest(bfx.client, "GET",
		fmt.Sprintf(URL, granularity, pair, size, since, end), "", map[string]string{
			"Content-Type": "application/json; charset=UTF-8",
		})

	if err != nil {
		return nil, err
	}

	var response []interface{}
	err = json.Unmarshal(respData, &response)
	if err != nil {
		return nil, err
	}

	var klines []goex.Kline
	for _, _itm := range response {
		itm := _itm.([]interface{})
		klines = append(klines, goex.Kline{
			Timestamp: int64(ToFloat64(itm[0])),
			Pair:      currencyPair,
			Open:      ToFloat64(itm[1]),
			Close:     ToFloat64(itm[2]),
			High:      ToFloat64(itm[3]),
			Low:       ToFloat64(itm[4]),
			Vol:       ToFloat64(itm[5])})
	}
	return klines, nil
}

func ToFloat64(v interface{}) float64 {
	if v == nil {
		return 0.0
	}

	switch v.(type) {
	case float64:
		return v.(float64)
	case string:
		vStr := v.(string)
		vF, _ := strconv.ParseFloat(vStr, 64)
		return vF
	default:
		panic("to float64 error.")
	}
}

func NewHttpRequest(client *http.Client, reqType string, reqUrl string, postData string, requstHeaders map[string]string) ([]byte, error) {
	req, err := http.NewRequest(reqType, reqUrl, strings.NewReader(postData))
	if err != nil {
		return nil, err
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36")
	}
	req.Header.Set("Accept-Encoding", "gzip")

	if requstHeaders != nil {
		for k, v := range requstHeaders {
			req.Header.Add(k, v)
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	var reader io.ReadCloser
	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		reader, _ = gzip.NewReader(resp.Body)
		defer reader.Close()
	default:
		reader = resp.Body
	}

	bodyData, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HttpStatusCode:%d ,Desc:%s", resp.StatusCode, string(bodyData))
	}

	return bodyData, nil
}
