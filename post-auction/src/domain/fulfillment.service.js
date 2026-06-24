const crypto = require("crypto");

const pool = require("../db/pool");
const producer = require("../kafka/producer");
const topics = require("../kafka/topics");
const { getIo } = require("../socket");

const DEFAULT_COMMISSION_RATE = 0.1;
const DEFAULT_PICKUP_LOCATION = "Fish Auction Pickup Point";
const NEARBY_KEYWORDS = [
  "iyte",
  "izmir",
  // Urla ve mahalleleri/köyleri
  "urla",
  "gulbahce",
  "zeytineli",
  "balikliova",
  "ozbek",
  "bademler",
  "barbaros",
  "kuscular",
  "demircili",
  // Çevre ilçeler / yarımada
  "guzelbahce",
  "karaburun",
  "mordogan",
  "cesme",
  "seferihisar"
];

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function toIso(value) {
  if (!value) return new Date().toISOString();
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? new Date().toISOString() : date.toISOString();
}

function normalizeEventId(value) {
  return value || crypto.randomUUID();
}

function createFulfillmentService(deps = {}) {
  const db = deps.pool || pool;
  const kafkaProducer = deps.producer || producer;
  const socketProvider = deps.getIo || getIo;
  const commissionRate = deps.commissionRate ?? DEFAULT_COMMISSION_RATE;

  async function publishEvent(client, eventType, aggregateId, payload, topic, key) {
    const eventId = crypto.randomUUID();
    const occurredAt = new Date().toISOString();
    const eventPayload = {
      ...payload,
      eventId,
      occurredAt
    };

    await client.query(
      `
      INSERT INTO event_store (event_id, aggregate_type, aggregate_id, event_type, payload, topic, key, occurred_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      `,
      [eventId, "fulfillment", aggregateId, eventType, eventPayload, topic, key || payload.sessionId, occurredAt]
    );

    return { topic, key: key || payload.sessionId, payload: eventPayload };
  }

  // Tracks in-flight background publishes so tests (and graceful shutdown) can
  // wait for them. Production request handlers never await these.
  const pendingPublishes = new Set();

  async function flushEvents(events) {
    for (const event of events) {
      try {
        await kafkaProducer.publish(event.topic, event.payload, event.key);
        await db.query("UPDATE event_store SET published_at = NOW(), publish_error = NULL WHERE event_id = $1", [event.payload.eventId]);
      } catch (error) {
        try {
          await db.query("UPDATE event_store SET publish_error = $2 WHERE event_id = $1", [event.payload.eventId, error.message]);
        } catch (_) {
          // best-effort; the event_store row stays unpublished for later retry
        }
      }
    }
  }

  async function withTx(action) {
    const client = await db.connect();
    const events = [];
    let result;
    let committed = false;

    try {
      await client.query("BEGIN");
      result = await action(client, async (...args) => {
        const event = await publishEvent(client, ...args);
        events.push(event);
        return event;
      });
      await client.query("COMMIT");
      committed = true;
    } catch (error) {
      if (!committed) {
        try { await client.query("ROLLBACK"); } catch (_) { /* ignore */ }
      }
      throw error;
    } finally {
      // Release the pooled connection BEFORE publishing so a slow/unhealthy
      // Kafka broker can never hold a DB connection or block the HTTP response.
      client.release();
    }

    // The event_store row is already committed (durable outbox); publish to
    // Kafka in the background. Failures leave the row unpublished for retry,
    // but they no longer slow down the request that triggered them.
    if (events.length) {
      const job = flushEvents(events).catch(() => {});
      pendingPublishes.add(job);
      job.then(() => pendingPublishes.delete(job));
    }

    return result;
  }

  async function flushPublishes() {
    await Promise.allSettled([...pendingPublishes]);
  }

  async function markProcessed(client, eventId, topic) {
    const normalized = normalizeEventId(eventId);
    const result = await client.query(
      "INSERT INTO processed_events (event_id, topic) VALUES ($1, $2) ON CONFLICT (event_id) DO NOTHING",
      [normalized, topic]
    );
    return result.rowCount === 1;
  }

  function emit(sessionId, eventName, payload) {
    try {
      socketProvider().to(sessionId).emit(eventName, payload);
      socketProvider().emit("fulfillmentUpdated", payload);
    } catch (error) {
      // Socket.IO is optional in tests and during early startup.
    }
  }

  async function getSessionStatus(client, sessionId) {
    const result = await client.query(
      "SELECT status FROM fulfillment_sessions WHERE session_id = $1",
      [sessionId]
    );
    return result.rows[0] ? result.rows[0].status : "OPEN";
  }

  async function assertSessionOpen(client, sessionId) {
    const status = await getSessionStatus(client, sessionId);
    if (status === "CLOSED") {
      const error = new Error("This auction is closed; no further fulfillment actions are allowed.");
      error.code = "SESSION_CLOSED";
      throw error;
    }
  }

  async function getSessionIdByBasket(client, basketId) {
    const result = await client.query(
      "SELECT session_id FROM fulfillment_sales WHERE basket_id = $1",
      [basketId]
    );
    if (result.rowCount === 0) {
      throw new Error("Sale not found");
    }
    return result.rows[0].session_id;
  }

  async function handleBuyerRegistered(payload) {
    const buyerId = payload.buyerId || payload.id;
    if (!buyerId) return;

    await db.query(
      `
      INSERT INTO buyers_projection (buyer_id, name, email, phone, address, occurred_at, raw_payload)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (buyer_id) DO UPDATE SET
        name = EXCLUDED.name,
        email = EXCLUDED.email,
        phone = EXCLUDED.phone,
        address = EXCLUDED.address,
        occurred_at = EXCLUDED.occurred_at,
        raw_payload = EXCLUDED.raw_payload
      `,
      [
        buyerId,
        payload.name || payload.username || "Unknown Buyer",
        payload.email || null,
        payload.phone || null,
        payload.address || null,
        toIso(payload.occurredAt),
        payload
      ]
    );
  }

  async function handleMemberRegistered(payload) {
    const memberId = payload.memberId || payload.captainId || payload.id;
    if (!memberId) return;

    await db.query(
      `
      INSERT INTO members_projection (member_id, member_name, boat_name, email, phone, occurred_at, raw_payload)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (member_id) DO UPDATE SET
        member_name = EXCLUDED.member_name,
        boat_name = EXCLUDED.boat_name,
        email = EXCLUDED.email,
        phone = EXCLUDED.phone,
        occurred_at = EXCLUDED.occurred_at,
        raw_payload = EXCLUDED.raw_payload
      `,
      [
        memberId,
        payload.memberName || payload.name || "Unknown Captain",
        payload.boatName || "Unknown Boat",
        payload.email || null,
        payload.phone || null,
        toIso(payload.occurredAt),
        payload
      ]
    );
  }

  async function handleCatalogPublished(payload) {
    const sessionId = payload.sessionId || payload.id;
    if (!sessionId) return;

    // The auction catalog has gone live; open the session in our projection so
    // it shows up (with title and expected basket count) even before any sale.
    await db.query(
      `
      INSERT INTO fulfillment_sessions (session_id, title, total_baskets, status, updated_at)
      VALUES ($1, $2, $3, 'OPEN', NOW())
      ON CONFLICT (session_id) DO UPDATE SET
        title = COALESCE(EXCLUDED.title, fulfillment_sessions.title),
        total_baskets = COALESCE(EXCLUDED.total_baskets, fulfillment_sessions.total_baskets),
        updated_at = NOW()
      `,
      [
        sessionId,
        payload.title || null,
        payload.totalBaskets != null ? Number(payload.totalBaskets) : (Array.isArray(payload.basketIds) ? payload.basketIds.length : null)
      ]
    );

    emit(sessionId, "catalogPublished", { sessionId });
  }

  async function handleBasketCreated(payload) {
    const basketId = payload.basketId;
    if (!basketId) return;

    await db.query(
      `
      INSERT INTO catalog_baskets_projection (basket_id, species, quantity, unit, quality, base_price, boat_name, member_id, occurred_at, raw_payload)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      ON CONFLICT (basket_id) DO UPDATE SET
        species = EXCLUDED.species,
        quantity = EXCLUDED.quantity,
        unit = EXCLUDED.unit,
        quality = EXCLUDED.quality,
        base_price = EXCLUDED.base_price,
        boat_name = EXCLUDED.boat_name,
        member_id = EXCLUDED.member_id,
        occurred_at = EXCLUDED.occurred_at,
        raw_payload = EXCLUDED.raw_payload
      `,
      [
        basketId,
        payload.species || null,
        payload.quantity || null,
        payload.unit || null,
        payload.quality || null,
        payload.basePrice || null,
        payload.boatName || null,
        payload.memberId || null,
        toIso(payload.occurredAt),
        payload
      ]
    );
  }

  async function handleBasketSold(payload) {
    return withTx(async (client, registerEvent) => {
      const isNew = await markProcessed(client, payload.eventId, topics.consumedTopics.BID_BASKET_SOLD);
      if (!isNew) {
        return { duplicate: true };
      }

      const catalog = await client.query("SELECT boat_name, member_id FROM catalog_baskets_projection WHERE basket_id = $1", [payload.basketId]);
      const basketProjection = catalog.rows[0] || {};
      const saleId = crypto.randomUUID();
      const occurredAt = toIso(payload.occurredAt);

      const result = await client.query(
        `
        INSERT INTO fulfillment_sales (
          sale_id, session_id, basket_id, buyer_id, winning_bid_id, sale_price,
          boat_name, member_id, sale_status, fulfillment_status, recorded_at, updated_at, raw_payload
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'RECORDED', 'PENDING', $9, NOW(), $10)
        ON CONFLICT (basket_id) DO UPDATE SET
          session_id = EXCLUDED.session_id,
          buyer_id = EXCLUDED.buyer_id,
          winning_bid_id = EXCLUDED.winning_bid_id,
          sale_price = EXCLUDED.sale_price,
          boat_name = COALESCE(EXCLUDED.boat_name, fulfillment_sales.boat_name),
          member_id = COALESCE(EXCLUDED.member_id, fulfillment_sales.member_id),
          raw_payload = EXCLUDED.raw_payload,
          updated_at = NOW()
        RETURNING *
        `,
        [
          saleId,
          payload.sessionId,
          payload.basketId,
          payload.buyerId,
          payload.winningBidId,
          payload.salePrice,
          basketProjection.boat_name || null,
          basketProjection.member_id || null,
          occurredAt,
          payload
        ]
      );

      await registerEvent(
        "fulfillment.sale.recorded",
        payload.basketId,
        {
          sessionId: payload.sessionId,
          basketId: payload.basketId,
          buyerId: payload.buyerId,
          winningBidId: payload.winningBidId,
          salePrice: toNumber(payload.salePrice)
        },
        topics.publishedTopics.FULFILLMENT_SALE_RECORDED,
        payload.sessionId
      );

      emit(payload.sessionId, "saleRecorded", { sessionId: payload.sessionId, basketId: payload.basketId });
      return result.rows[0];
    });
  }

  async function schedulePickup(basketId, pickupLocation, pickupTimeWindow) {
    return withTx(async (client, registerEvent) => {
      await assertSessionOpen(client, await getSessionIdByBasket(client, basketId));
      const result = await client.query(
        `
        UPDATE fulfillment_sales
        SET pickup_location = $2,
            pickup_time_window = $3,
            fulfillment_status = CASE WHEN fulfillment_status = 'PENDING' THEN 'PICKUP_SCHEDULED' ELSE fulfillment_status END,
            pickup_scheduled_at = NOW(),
            updated_at = NOW()
        WHERE basket_id = $1
        RETURNING *
        `,
        [basketId, pickupLocation || DEFAULT_PICKUP_LOCATION, pickupTimeWindow]
      );

      if (result.rowCount === 0) {
        throw new Error("Sale not found");
      }

      const sale = result.rows[0];
      await registerEvent(
        "fulfillment.pickup.scheduled",
        basketId,
        {
          sessionId: sale.session_id,
          basketId: sale.basket_id,
          buyerId: sale.buyer_id,
          method: "PICKUP",
          deliveryAddress: sale.delivery_address || undefined,
          deliverable: sale.delivery_available === null || sale.delivery_available === undefined ? undefined : Boolean(sale.delivery_available)
        },
        topics.publishedTopics.FULFILLMENT_PICKUP_SCHEDULED,
        sale.session_id
      );

      emit(sale.session_id, "pickupScheduled", { sessionId: sale.session_id, basketId });
      return sale;
    });
  }

  function normalizeText(value) {
    return String(value || "")
      .toLowerCase()
      .replace(/ı/g, "i")
      .replace(/ş/g, "s")
      .replace(/ğ/g, "g")
      .replace(/ç/g, "c")
      .replace(/ö/g, "o")
      .replace(/ü/g, "u");
  }

  function checkDeliveryAvailability(address) {
    const normalized = normalizeText(address);
    if (!normalized.trim()) {
      return { available: false, reason: "ADDRESS_MISSING" };
    }

    const available = NEARBY_KEYWORDS.some((keyword) => normalized.includes(keyword));
    return {
      available,
      reason: available ? "NEARBY_ADDRESS" : "OUT_OF_DELIVERY_AREA"
    };
  }

  async function checkDelivery(basketId, address) {
    return withTx(async (client, registerEvent) => {
      await assertSessionOpen(client, await getSessionIdByBasket(client, basketId));
      const saleResult = await client.query(
        `
        SELECT fs.*, bp.address AS buyer_address
        FROM fulfillment_sales fs
        LEFT JOIN buyers_projection bp ON bp.buyer_id = fs.buyer_id
        WHERE fs.basket_id = $1
        `,
        [basketId]
      );

      if (saleResult.rowCount === 0) {
        throw new Error("Sale not found");
      }

      const sale = saleResult.rows[0];
      const deliveryAddress = address || sale.buyer_address || "";
      const delivery = checkDeliveryAvailability(deliveryAddress);

      const updateResult = await client.query(
        `
        UPDATE fulfillment_sales
        SET delivery_address = $2,
            delivery_available = $3,
            delivery_reason = $4,
            delivery_checked_at = NOW(),
            updated_at = NOW()
        WHERE basket_id = $1
        RETURNING *
        `,
        [basketId, deliveryAddress, delivery.available, delivery.reason]
      );

      const updatedSale = updateResult.rows[0];
      await registerEvent(
        "fulfillment.delivery.checked",
        basketId,
        {
          sessionId: updatedSale.session_id,
          basketId: updatedSale.basket_id,
          buyerId: updatedSale.buyer_id,
          address: deliveryAddress,
          deliveryAvailable: delivery.available,
          reason: delivery.reason
        },
        topics.publishedTopics.FULFILLMENT_DELIVERY_CHECKED,
        updatedSale.session_id
      );

      emit(updatedSale.session_id, "deliveryChecked", { sessionId: updatedSale.session_id, basketId });
      return updatedSale;
    });
  }

  async function completeBasket(basketId) {
    return withTx(async (client, registerEvent) => {
      await assertSessionOpen(client, await getSessionIdByBasket(client, basketId));
      const result = await client.query(
        `
        UPDATE fulfillment_sales
        SET fulfillment_status = 'COMPLETED',
            sale_status = 'COMPLETED',
            completed_at = NOW(),
            updated_at = NOW()
        WHERE basket_id = $1
        RETURNING *
        `,
        [basketId]
      );

      if (result.rowCount === 0) {
        throw new Error("Sale not found");
      }

      const sale = result.rows[0];
      await registerEvent(
        "fulfillment.basket.completed",
        basketId,
        {
          sessionId: sale.session_id,
          basketId: sale.basket_id,
          buyerId: sale.buyer_id,
          fulfillmentStatus: "COMPLETED",
          deliveryAvailable: Boolean(sale.delivery_available)
        },
        topics.publishedTopics.FULFILLMENT_BASKET_COMPLETED,
        sale.session_id
      );

      // The basket is now empty (all fish sold), so the captain's share for
      // this basket is calculated immediately, mirroring how a fish auction
      // settles each lot as it finishes.
      const boatName = sale.boat_name || "Unknown Boat";
      const memberLookup = await client.query(
        "SELECT member_id, member_name FROM members_projection WHERE member_id = $1 OR boat_name = $2 LIMIT 1",
        [sale.member_id, boatName]
      );
      const captainName = memberLookup.rows[0] ? memberLookup.rows[0].member_name : null;
      const resolvedMemberId = sale.member_id || (memberLookup.rows[0] ? memberLookup.rows[0].member_id : null);

      const grossAmount = toNumber(sale.sale_price);
      const commissionAmount = Number((grossAmount * commissionRate).toFixed(2));
      const netAmount = Number((grossAmount - commissionAmount).toFixed(2));
      const paymentId = crypto.randomUUID();

      const payment = await client.query(
        `
        INSERT INTO captain_payments (
          captain_payment_id, session_id, basket_id, member_id, captain_name, boat_name,
          gross_amount, commission_amount, net_amount, status, calculated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'CALCULATED', NOW())
        ON CONFLICT (session_id, basket_id) DO UPDATE SET
          member_id = EXCLUDED.member_id,
          captain_name = EXCLUDED.captain_name,
          boat_name = EXCLUDED.boat_name,
          gross_amount = EXCLUDED.gross_amount,
          commission_amount = EXCLUDED.commission_amount,
          net_amount = EXCLUDED.net_amount,
          calculated_at = NOW()
        RETURNING *
        `,
        [paymentId, sale.session_id, basketId, resolvedMemberId, captainName, boatName, grossAmount, commissionAmount, netAmount]
      );
      const paymentRow = payment.rows[0];

      // The Notification contract expects a per-captain aggregate (how many
      // baskets and the total owed), so we roll up this captain's baskets in
      // the session even though we store each basket's payout individually.
      const agg = await client.query(
        "SELECT COUNT(*)::int AS cnt, COALESCE(SUM(net_amount), 0)::numeric AS total FROM captain_payments WHERE session_id = $1 AND boat_name = $2",
        [sale.session_id, boatName]
      );
      const soldBasketCount = Number(agg.rows[0].cnt || 0);
      const totalAmount = toNumber(agg.rows[0].total);

      await registerEvent(
        "fulfillment.captain.payment.calculated",
        sale.session_id,
        {
          sessionId: sale.session_id,
          memberId: paymentRow.member_id || undefined,
          boatName: paymentRow.boat_name,
          soldBasketCount,
          totalAmount
        },
        topics.publishedTopics.FULFILLMENT_CAPTAIN_PAYMENT_CALCULATED,
        sale.session_id
      );

      emit(sale.session_id, "basketCompleted", { sessionId: sale.session_id, basketId });
      return sale;
    });
  }

  async function closeAuction(sessionId) {
    return withTx(async (client, registerEvent) => {
      const existing = await client.query(
        "SELECT * FROM fulfillment_sessions WHERE session_id = $1",
        [sessionId]
      );
      if (existing.rows[0] && existing.rows[0].status === "CLOSED") {
        const row = existing.rows[0];
        return {
          sessionId,
          totalSales: Number(row.total_sales || 0),
          totalRevenue: toNumber(row.total_revenue),
          totalCaptainPayments: toNumber(row.total_captain_payments),
          closedAt: toIso(row.closed_at),
          alreadyClosed: true
        };
      }

      // A fish auction closes only after every basket is finished. Block the
      // close while any basket is still pending so the books stay consistent.
      const pending = await client.query(
        "SELECT COUNT(*)::int AS n FROM fulfillment_sales WHERE session_id = $1 AND fulfillment_status <> 'COMPLETED'",
        [sessionId]
      );
      const pendingCount = Number(pending.rows[0].n || 0);
      if (pendingCount > 0) {
        const error = new Error(`Cannot close auction: ${pendingCount} basket(s) not completed yet.`);
        error.code = "SESSION_HAS_PENDING";
        throw error;
      }

      const summary = await client.query(
        `
        SELECT
          COUNT(*)::int AS total_sales,
          COALESCE(SUM(sale_price), 0)::numeric AS total_revenue
        FROM fulfillment_sales
        WHERE session_id = $1
        `,
        [sessionId]
      );
      const payments = await client.query(
        "SELECT COALESCE(SUM(net_amount), 0)::numeric AS total_captain_payments FROM captain_payments WHERE session_id = $1",
        [sessionId]
      );

      const totalSales = Number(summary.rows[0].total_sales || 0);
      const totalRevenue = toNumber(summary.rows[0].total_revenue);
      const totalCaptainPayments = toNumber(payments.rows[0].total_captain_payments);
      const closedAt = new Date().toISOString();

      // Unsold = expected baskets (from catalog.published) minus the sold ones.
      const expectedRow = await client.query(
        "SELECT total_baskets FROM fulfillment_sessions WHERE session_id = $1",
        [sessionId]
      );
      const expectedBaskets = expectedRow.rows[0] && expectedRow.rows[0].total_baskets != null
        ? Number(expectedRow.rows[0].total_baskets)
        : totalSales;
      const unsoldBasketCount = Math.max(0, expectedBaskets - totalSales);

      await client.query(
        `
        INSERT INTO fulfillment_sessions (
          session_id, status, total_sales, total_revenue, total_captain_payments, closed_at, updated_at
        )
        VALUES ($1, 'CLOSED', $2, $3, $4, $5, NOW())
        ON CONFLICT (session_id) DO UPDATE SET
          status = 'CLOSED',
          total_sales = EXCLUDED.total_sales,
          total_revenue = EXCLUDED.total_revenue,
          total_captain_payments = EXCLUDED.total_captain_payments,
          closed_at = EXCLUDED.closed_at,
          updated_at = NOW()
        `,
        [sessionId, totalSales, totalRevenue, totalCaptainPayments, closedAt]
      );

      await registerEvent(
        "auction.closed",
        sessionId,
        {
          sessionId,
          soldBasketCount: totalSales,
          unsoldBasketCount,
          totalRevenue,
          closedAt
        },
        topics.publishedTopics.FULFILLMENT_AUCTION_CLOSED,
        sessionId
      );

      emit(sessionId, "auctionClosed", { sessionId });
      return { sessionId, totalSales, totalRevenue, totalCaptainPayments, closedAt };
    });
  }

  async function handleAllBasketsFinalized(payload) {
    return withTx(async (client) => {
      await markProcessed(client, payload.eventId, topics.consumedTopics.BID_ALL_BASKETS_FINALIZED);
      return payload;
    });
  }

  async function getFulfillmentData() {
    const [sales, payments, sessions] = await Promise.all([
      db.query(
        `
        SELECT fs.*, bp.name AS buyer_name, bp.address AS buyer_address, cbp.species, cbp.quantity, cbp.unit, cbp.quality
        FROM fulfillment_sales fs
        LEFT JOIN buyers_projection bp ON bp.buyer_id = fs.buyer_id
        LEFT JOIN catalog_baskets_projection cbp ON cbp.basket_id = fs.basket_id
        ORDER BY fs.recorded_at DESC
        `
      ),
      db.query("SELECT * FROM captain_payments ORDER BY boat_name, calculated_at"),
      db.query("SELECT * FROM fulfillment_sessions")
    ]);

    return {
      sales: sales.rows,
      captainPayments: payments.rows,
      sessions: sessions.rows
    };
  }

  async function getFulfillmentSnapshot() {
    const result = await db.query(
      `
      SELECT
        (SELECT COUNT(*)::int FROM fulfillment_sales) AS count,
        GREATEST(
          (SELECT COALESCE(MAX(updated_at), '1970-01-01'::timestamptz) FROM fulfillment_sales),
          (SELECT COALESCE(MAX(updated_at), '1970-01-01'::timestamptz) FROM fulfillment_sessions)
        ) AS updated_at
      `
    );
    const row = result.rows[0];
    return { token: `${row.count}:${new Date(row.updated_at).toISOString()}` };
  }

  return {
    handleBuyerRegistered,
    handleMemberRegistered,
    handleCatalogPublished,
    handleBasketCreated,
    handleBasketSold,
    handleAllBasketsFinalized,
    schedulePickup,
    checkDelivery,
    checkDeliveryAvailability,
    completeBasket,
    closeAuction,
    getFulfillmentData,
    getFulfillmentSnapshot,
    flushPublishes
  };
}

module.exports = createFulfillmentService();
module.exports.createFulfillmentService = createFulfillmentService;
