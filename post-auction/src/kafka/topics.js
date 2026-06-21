module.exports = {
  consumedTopics: {
    USER_BUYER_REGISTERED: "user.buyer.registered",
    USER_MEMBER_REGISTERED: "user.member.registered",
    CATALOG_BASKET_CREATED: "catalog.basket.created",
    CATALOG_PUBLISHED: "catalog.published",
    BID_BASKET_SOLD: "bid.basket.sold",
    BID_ALL_BASKETS_FINALIZED: "bid.all.baskets.finalized"
  },
  publishedTopics: {
    AUCTION_SESSION_STARTED: "auction.session.started",
    AUCTION_BASKET_OPENED: "auction.basket.opened",
    BID_PLACED: "bid.placed",
    BID_BASKET_SOLD: "bid.basket.sold",
    BID_BASKET_UNSOLD: "bid.basket.unsold",
    BID_REBID_ROUND_OPENED: "bid.rebid.round.opened",
    BID_ALL_BASKETS_FINALIZED: "bid.all.baskets.finalized",
    FULFILLMENT_SALE_RECORDED: "fulfillment.sale.recorded",
    FULFILLMENT_PICKUP_SCHEDULED: "fulfillment.pickup.scheduled",
    FULFILLMENT_DELIVERY_CHECKED: "fulfillment.delivery.checked",
    FULFILLMENT_BASKET_COMPLETED: "fulfillment.basket.completed",
    FULFILLMENT_CAPTAIN_PAYMENT_CALCULATED: "fulfillment.captain.payment.calculated",
    FULFILLMENT_AUCTION_CLOSED: "fulfillment.auction.closed"
  }
};
