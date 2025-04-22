# ⚠️ Deprecated: CoinCap API v2 No Longer Supported

## 📊 Real-Time Crypto Tracker with Kafka and CoinCap
A Go script that collects real-time cryptocurrency data from the CoinCap API and streams updates through Apache Kafka. The project uses a Dockerized Kafka container to relay updates in real time, tracking selected cryptocurrencies.

## 🛠️ Installation & Setup

1. docker run -d --name=kafka -p 9092:9092 apache/kafka or use your own local one
2. Set up .env file with kafka port
3. run the script
