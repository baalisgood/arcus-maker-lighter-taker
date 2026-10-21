"""An exchange write may have executed; do not replay it or compensate blindly."""
class OrderStateUnknownError(RuntimeError):
    def __init__(self, exchange, order_id=None, client_id=None):
        self.exchange = exchange
        self.order_id = order_id
        self.client_id = client_id
        super().__init__(f"{exchange}: order state unknown; manual reconciliation required")


class OrderAdmissionDeferredError(ValueError):
    """An internal pre-sign check proved that this intent was not signed or sent."""


class OrderSendRejectedError(ValueError):
    """The venue API explicitly refused a signed write (HTTP 4xx except 429, or error code without tx hash).

    The transaction was not admitted, so no fill can exist for it. `detail` carries status/code/message.
    """
    def __init__(self, venue, detail):
        super().__init__(f"{venue}: write explicitly rejected: {detail}")
        self.venue = venue
        self.detail = detail
