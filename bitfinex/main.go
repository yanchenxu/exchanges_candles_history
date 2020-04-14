package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"time"

	"github.com/nntaoli-project/goex"
)

var (
	klinePeriod = goex.KLINE_PERIOD_1MIN //see: github.com/nntaoli-project/GoEx/Const.go

	endTime = time.Date(2020, 4, 14, 0, 0, 0, 0, time.Local)

	//beginTime = time.Date(2013, 4, 2, 0, 0, 0, 0, time.Local)
	// currencyPair = goex.BTC_USDT
	// dataDir      = "data/btc"
	// 2014.11.23 -2014.12.31 缺少1分时数据
	// 2015.5.1 -2015.5.30 缺少1分时数据
	// 2016.4.1-2016.5.30 缺少1分时数据
	// 2016.8.3-2016.8.9 缺少1分时数据

	// beginTime    = time.Date(2016, 3, 10, 0, 0, 0, 0, time.Local)
	// currencyPair = goex.ETH_USDT
	// dataDir      = "data/eth"
	// 2016.4.01 -2016.5.31 缺少1分时数据
	// 2016.8.03 -2016.8.9 缺少1分时数据

	// beginTime    = time.Date(2014, 1, 25, 0, 0, 0, 0, time.Local)
	// currencyPair = goex.LTC_USDT
	// dataDir      = "data/ltc"
	// 2013.5.20 -2013.5.21 缺少1分时数据
	// 2013.6.1
	// 2013.6.6
	// 2013.6.8
	// 2013-07-20
	// 2013-07-26
	// 2013-07-29
	// 2013-08-01
	// 2013-08-03
	// 2013-08-06
	// 2013-08-07
	// 2013-08-17
	// 2013-09-11
	// 2013-09-13 缺少1分时数据
	// 2014-11-22 - 2014-12-31 缺少1分时数据
	// 2015-05-01 - 2015-05-30 缺少1分时数据
	// 2016-04-02 - 2016-05-31 缺少1分时数据
	// 2016-08-03 - 2016-08-09 缺少1分时数据

	// beginTime    = time.Date(2017, 7, 2, 0, 0, 0, 0, time.Local)
	// currencyPair = goex.EOS_USDT
	// dataDir      = "data/eos"

	// beginTime    = time.Date(2017, 8, 2, 0, 0, 0, 0, time.Local)
	// currencyPair = goex.BCH_USDT
	// dataDir      = "data/bch"

	// beginTime    = time.Date(2018, 11, 13, 0, 0, 0, 0, time.Local)
	// currencyPair = goex.BSV_USDT
	// dataDir      = "data/bsv"

	beginTime    = time.Date(2016, 7, 27, 0, 0, 0, 0, time.Local)
	currencyPair = goex.ETC_USDT
	dataDir      = "data/etc"

	csvWriterM map[string]*csv.Writer
	fileM      map[string]*os.File
)

func init() {
	csvWriterM = make(map[string]*csv.Writer, 10)
	fileM = make(map[string]*os.File, 10)
}

func csvWriter(timestamp int64) *csv.Writer {
	t := time.Unix(timestamp/1000, 0).Format("2006-01-02")
	p := "1min"
	switch klinePeriod {
	case goex.KLINE_PERIOD_1MIN:
		p = "1min"
	case goex.KLINE_PERIOD_5MIN:
		p = "5min"
	case goex.KLINE_PERIOD_30MIN:
		p = "30min"
	case goex.KLINE_PERIOD_1H:
		p = "1h"
	case goex.KLINE_PERIOD_4H:
		p = "4h"
	case goex.KLINE_PERIOD_1DAY:
		p = "1day"
	}
	fileName := fmt.Sprintf("bitfinex_kline_%s_%s_%s.csv", currencyPair.ToLower().ToSymbol(""), p, t)

	w := csvWriterM[fileName]
	if w != nil {
		return w
	}

	fpath := path.Join(dataDir, fileName)
	f, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0766)
	if err != nil {
		panic(err)
	}

	w = csv.NewWriter(f)

	csvWriterM[fileName] = w
	fileM[fileName] = f

	return w
}

func main() {
	log.Println("begin download kline")

	os.MkdirAll(dataDir, 0700)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, os.Kill)
		<-c
		cancel()
	}()

	defer func() {
		for _, w := range csvWriterM {
			w.Flush()
		}

		for _, f := range fileM {
			f.Close()
		}

		log.Println("end")
	}()

	ba := NewBitfinex(&http.Client{
		Transport: &http.Transport{
			Proxy: func(request *http.Request) (*url.URL, error) {
				return url.Parse("socks5://127.0.0.1:1086") //ss proxy
			},
		},
		Timeout: 10 * time.Second,
	}, "", "")

	since := int(beginTime.Unix()) * 1000
	interval := time.NewTimer(200 * time.Millisecond)

	for {
		select {
		case <-ctx.Done():
			return
		case <-interval.C:
			klines, err := ba.GetKlineRecords(currencyPair, klinePeriod, 1500, since, since+60*60*24*1000-1)
			if err != nil {
				log.Println(err)
				interval.Reset(200 * time.Millisecond)
				continue
			}

			if len(klines) == 0 {
				log.Printf("no klines, jump %s ", time.Unix(int64(since/1000), 0).Format("2006-01-02"))
				since = since + 60*60*24*1000
				interval.Reset(200 * time.Millisecond)
				continue
			}

			for _, k := range klines {
				csvWriter(k.Timestamp).Write([]string{fmt.Sprint(k.Timestamp), goex.FloatToString(k.High, 8),
					goex.FloatToString(k.Low, 8), goex.FloatToString(k.Open, 8), goex.FloatToString(k.Close, 8), goex.FloatToString(k.Vol, 8)})
			}

			since = int(klines[len(klines)-1].Timestamp) + 1
			if since > int(endTime.Unix())*1000 {
				cancel()
			}

			interval.Reset(200 * time.Millisecond)
		}
	}
}
