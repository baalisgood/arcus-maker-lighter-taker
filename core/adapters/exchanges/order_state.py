"""An exchange write may have executed; do not replay it or compensate blindly."""
class OrderStateUnknownError(RuntimeError):
    def __init__(self, exchange, order_id=None, client_id=None):
        self.exchange = exchange
        self.order_id = order_id
        self.client_id = client_id
        super().__init__(f"{exchange}: order state unknown; manual reconciliation required")


class OrderAdmissionDeferredError(ValueError):
    """An internal pre-sign check proved that this intent was not signed or sent."""
