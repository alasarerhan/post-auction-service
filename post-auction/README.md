# Post-Auction & Fulfillment Service

This directory hosts the **Post-Auction & Fulfillment Service** as an isolated, deployable microservice. It is the Service 4 in the Online Fish Auction System.

## Responsibilities

- Recording sales when a basket is sold
- Managing pickup information
- Checking whether delivery is possible for nearby addresses
- Calculating the amount to be paid to each captain
- Closing the auction (post-auction finalization)

## Directory layout

```text
post-auction/
├── package.json
├── package-lock.json
├── Dockerfile
├── .dockerignore
├── .env.example
├── .gitignore
├── README.md
├── src/
│   ├── app.js
│   ├── controllers/fulfillment.controller.js
│   ├── routes/fulfillment.routes.js
│   ├── domain/fulfillment.service.js
│   ├── db/{init,pool,schema.sql}
│   ├── kafka/{config,consumer,producer,schema-registry,topics}.js
│   ├── views/{fulfillment.ejs,index.ejs}
│   ├── public/main.js
│   └── socket.js
├── schema/
│   ├── fulfillment.*.schema.json   (6 files)
│   └── user.member.registered.schema.json
└── test/
    ├── fulfillment.unit.test.js
    └── fulfillment.integration.test.js
```

## Kafka contract

### Consume

- `user.buyer.registered`
- `user.member.registered`
- `catalog.basket.created`
- `catalog.published`
- `bid.basket.sold`
- `bid.all.baskets.finalized`

### Publish

- `fulfillment.sale.recorded`
- `fulfillment.pickup.scheduled`
- `fulfillment.delivery.checked`
- `fulfillment.basket.completed`
- `fulfillment.captain.payment.calculated`
- `fulfillment.auction.closed`

Kafka group: `post-auction-service-local` (configurable via `KAFKA_GROUP_ID`).
Kafka client: `post-auction-service` (configurable via `KAFKA_CLIENT_ID`).

## HTTP API

| Method | Path | Purpose |
|---|---|---|
| GET | `/` | Dashboard index |
| GET | `/fulfillment` | Post-Auction dashboard UI |
| GET | `/api/fulfillment/snapshot` | Local projections + sales snapshot |
| GET | `/health` | Liveness check |
| POST | `/fulfillment/sales/:basketId/pickup` | Schedule pickup |
| POST | `/fulfillment/sales/:basketId/delivery/check` | Run delivery availability check |
| POST | `/fulfillment/sales/:basketId/complete` | Mark basket fulfilled |
| POST | `/fulfillment/sessions/:sessionId/captain-payments/calculate` | Calculate captain payouts |
| POST | `/fulfillment/sessions/:sessionId/close` | Close auction session |

## Local setup

```bash
# 1. Start a private Postgres for Post-Auction
docker run --name post-auction-postgres \
  -e POSTGRES_DB=post_auction_service \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5433:5432 \
  -d postgres:16

# (later) just restart
docker start post-auction-postgres

# 2. Install deps
cd post-auction
npm install

# 3. Copy env template and fill Kafka + DB credentials
cp .env.example .env
# edit .env: KAFKA_BROKERS, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD, PGPASSWORD

# 4. Initialize schema
npm run db:init

# 5. Run
npm run dev
```

App listens on `PORT` (default `3000`). UI: `http://localhost:<port>/fulfillment`.

## Tests

```bash
cd post-auction
npm test
```

Coverage:

- Unit: delivery availability, captain payout math
- Integration: `bid.basket.sold` → sale recorded → pickup → delivery → complete → captain payment → auction close
- Outbox/event_store verification for all `fulfillment.*` topics
- Frontend render + API integration for `/fulfillment`

## Required Kafka topics

The cluster must contain these topics before live publishing succeeds:

```text
fulfillment.sale.recorded
fulfillment.pickup.scheduled
fulfillment.delivery.checked
fulfillment.basket.completed
fulfillment.captain.payment.calculated
fulfillment.auction.closed
```

If topic creation is restricted at the API key level, ask the Kafka admin team to provision them.

## 10-second verification

```bash
docker start post-auction-postgres
cd post-auction
npm test
curl -I http://localhost:3000/fulfillment
```

Expected: `npm test` reports all tests passing; `curl` returns `HTTP/1.1 200 OK`.
