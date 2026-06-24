const fulfillmentService = require("../domain/fulfillment.service");

async function renderWithError(res, next, error) {
  if (error && (error.code === "SESSION_CLOSED" || error.code === "SESSION_HAS_PENDING")) {
    try {
      const [dashboard, snapshot] = await Promise.all([
        fulfillmentService.getFulfillmentData(),
        fulfillmentService.getFulfillmentSnapshot()
      ]);
      return res.status(409).render("fulfillment", {
        dashboard,
        snapshotToken: snapshot.token,
        error: error.message,
        notice: null
      });
    } catch (renderError) {
      return next(renderError);
    }
  }
  return next(error);
}

async function renderFulfillmentDashboard(req, res, next) {
  try {
    const [dashboard, snapshot] = await Promise.all([
      fulfillmentService.getFulfillmentData(),
      fulfillmentService.getFulfillmentSnapshot()
    ]);

    res.render("fulfillment", {
      dashboard,
      snapshotToken: snapshot.token,
      error: null,
      notice: req.query.notice || null
    });
  } catch (error) {
    next(error);
  }
}

async function getFulfillmentSnapshot(req, res, next) {
  try {
    const snapshot = await fulfillmentService.getFulfillmentSnapshot();
    res.json(snapshot);
  } catch (error) {
    next(error);
  }
}

async function schedulePickup(req, res, next) {
  try {
    await fulfillmentService.schedulePickup(req.params.basketId, req.body.pickupLocation, req.body.pickupTimeWindow);
    res.redirect("/fulfillment?notice=pickup");
  } catch (error) {
    renderWithError(res, next, error);
  }
}

async function checkDelivery(req, res, next) {
  try {
    const sale = await fulfillmentService.checkDelivery(req.params.basketId, req.body.address);
    res.redirect("/fulfillment?notice=" + (sale && sale.delivery_available ? "delivery_ok" : "delivery_no"));
  } catch (error) {
    renderWithError(res, next, error);
  }
}

async function completeBasket(req, res, next) {
  try {
    await fulfillmentService.completeBasket(req.params.basketId);
    res.redirect("/fulfillment?notice=completed");
  } catch (error) {
    renderWithError(res, next, error);
  }
}

async function closeAuction(req, res, next) {
  try {
    const result = await fulfillmentService.closeAuction(req.params.sessionId);
    res.redirect("/fulfillment?notice=" + (result && result.alreadyClosed ? "already_closed" : "closed"));
  } catch (error) {
    renderWithError(res, next, error);
  }
}

module.exports = {
  renderFulfillmentDashboard,
  getFulfillmentSnapshot,
  schedulePickup,
  checkDelivery,
  completeBasket,
  closeAuction
};
