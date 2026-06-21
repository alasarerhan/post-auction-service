# Post-Auction Service — Branch `feat/post-auction-service`

Bu branch Auction & Bidding repo'sundan Post-Auction & Fulfillment modülünü izole eder. Aynı repoda farklı klasörde yaşar; deployable tek başına bir servistir.

## Dizin yapısı

```text
post-auction/
├── package.json
├── Dockerfile
├── .dockerignore
├── .env.example
├── .gitignore
├── src/
│   ├── app.js
│   ├── controllers/fulfillment.controller.js
│   ├── routes/fulfillment.routes.js
│   ├── domain/fulfillment.service.js
│   ├── db/
│   │   ├── init.js
│   │   ├── pool.js
│   │   └── schema.sql
│   ├── kafka/
│   │   ├── config.js
│   │   ├── consumer.js
│   │   ├── producer.js
│   │   ├── schema-registry.js
│   │   └── topics.js
│   ├── views/
│   │   ├── fulfillment.ejs
│   │   └── index.ejs
│   ├── public/main.js
│   └── socket.js
├── schema/
│   ├── fulfillment.auction.closed.schema.json
│   ├── fulfillment.basket.completed.schema.json
│   ├── fulfillment.captain.payment.calculated.schema.json
│   ├── fulfillment.delivery.checked.schema.json
│   ├── fulfillment.pickup.scheduled.schema.json
│   ├── fulfillment.sale.recorded.schema.json
│   └── user.member.registered.schema.json
└── test/
    ├── fulfillment.unit.test.js
    └── fulfillment.integration.test.js
```

## Kurulum (local)

```bash
docker run --name post-auction-postgres \
  -e POSTGRES_DB=post_auction_service \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5433:5432 \
  -d postgres:16

docker start post-auction-postgres

cd post-auction
npm install
cp .env.example .env
# .env içine KAFKA_BROKERS, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD doldur
npm run db:init
npm run dev
```

## Doğrulama

```bash
npm test
curl -I http://localhost:3000/health
curl -I http://localhost:3000/fulfillment
```

## API endpoints

```text
GET  /health
GET  /fulfillment
POST /fulfillment/sales/:basketId/pickup
POST /fulfillment/sales/:basketId/delivery/check
POST /fulfillment/sales/:basketId/complete
POST /fulfillment/sessions/:sessionId/captain-payments/calculate
POST /fulfillment/sessions/:sessionId/close
```

## Consume edilen Kafka topic'leri

```text
bid.basket.sold
bid.all.baskets.finalized
user.buyer.registered
user.member.registered
catalog.basket.created
```

## Publish edilen Kafka topic'leri

```text
fulfillment.sale.recorded
fulfillment.pickup.scheduled
fulfillment.delivery.checked
fulfillment.basket.completed
fulfillment.captain.payment.calculated
fulfillment.auction.closed
```

## Docker image build

```bash
docker build -t post-auction-service .
docker run --rm -p 3000:3000 \
  -e PORT=3000 \
  -e PGHOST=host.docker.internal \
  -e PGPORT=5433 \
  -e PGDATABASE=post_auction_service \
  -e PGUSER=postgres \
  -e PGPASSWORD=postgres \
  -e KAFKA_BROKERS=... \
  -e KAFKA_SASL_USERNAME=... \
  -e KAFKA_SASL_PASSWORD=... \
  -e KAFKA_GROUP_ID=post-auction-service-prod \
  -e KAFKA_CLIENT_ID=post-auction-service \
  post-auction-service
```

## Notlar

- Bu branch sadece Post-Auction modülünü içerir; Auction repo'sundaki diğer dosyalar değişmedi.
- `post-auction/` altındaki `src/kafka/schema-registry.js` içindeki schema yolları `../../../schema/...` şeklindedir (kafka → src → post-auction → schema).
- `src/domain/fulfillment.service.js` içindeki socket yolu `../socket` (kafka değil) şeklindedir.
- `docker` daemon bu ortamda kapalı olduğu için integration test Postgres bağlantısı yapamıyor; Docker açılınca testler tekrar geçer.
