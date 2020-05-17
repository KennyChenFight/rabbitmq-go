# rabbitmq-go
封裝rabbitmq client的library並實現producer and consumer機制

## 實現功能
+ 實現producer and consumer機制
+ 實現斷線重連機制
+ 實現push失敗，重新push機制
+ 實現consume失敗(指的不是consume成功而業務邏輯消費數據失敗的意思，而是可能因為連線失敗、或是queue名稱不對等原因)，重新consume機制

## How to use
+ 可參考main.go的使用方式
+ 基本上封裝的方式不難，可實際看producer.go與consumer.go的程式碼，並根據需求進行修正
+ 都有寫詳細註解

## Contribution
歡迎fork或issue，讓封裝的機制可以更彈性
