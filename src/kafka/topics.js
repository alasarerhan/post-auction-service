module.exports = {
  consumedTopics: {
    USER_BUYER_REGISTERED: "user.buyer.registered",
    CATALOG_BASKET_CREATED: "catalog.basket.created",
    CATALOG_PUBLISHED: "catalog.published"
  },
  publishedTopics: {
    AUCTION_SESSION_STARTED: "auction.session.started",
    AUCTION_BASKET_OPENED: "auction.basket.opened",
    BID_PLACED: "bid.placed",
    BID_BASKET_SOLD: "bid.basket.sold",
    BID_BASKET_UNSOLD: "bid.basket.unsold",
    BID_REBID_ROUND_OPENED: "bid.rebid.round.opened",
    BID_ALL_BASKETS_FINALIZED: "bid.all.baskets.finalized"
  }
};
