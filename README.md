
## 前処理
https://www.mizuhobank.co.jp/market/historical/index.html にある公示データ 2002~をダウンロードしてこのディレクトリに配置する


```
$ tail -n +4 src/main/resources/quote.csv > src/main/resources/quote_normalized.csv

```