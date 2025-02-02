import asyncio
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.max import max_constants as CONSTANTS, max_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.max.max_exchange import MaxExchange


class MaxAPIOrderBookDataSource(OrderBookTrackerDataSource):

    _logger: Optional[HummingbotLogger] = None

    def __init__(
            self,
            trading_pairs: List[str],
            connector: 'MaxExchange',
            api_factory: WebAssistantsFactory,
            domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._domain = domain
        self._api_factory = api_factory
        self._last_ws_message_sent_timestamp = 0
        self._ping_interval = 0

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot_response: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = snapshot_response['timestamp']
        update_id: int = snapshot_response["last_update_id"]

        order_book_message_content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": snapshot_response["bids"],
            "asks": snapshot_response["asks"]
        }
        snapshot_msg: OrderBookMessage = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            order_book_message_content,
            snapshot_timestamp)

        return snapshot_msg

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        params = {
            "market" : trading_pair,
            "limit" : CONSTANTS.DEPTH
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.SNAPSHOT_NO_AUTH_PATH_URL),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.SNAPSHOT_NO_AUTH_PATH_URL,
        )

        return data

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trade_data: Dict[str, Any] = raw_message["t"]
        timestamp: float = int(trade_data["time"]) * 1e-9
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=trade_data["symbol"])
        message_content = {
            "trade_id": raw_message["T"],
            "update_id": trade_data["sequence"],
            "trading_pair": trading_pair,
            "trade_type": float(TradeType.BUY.value) if trade_data["side"] == "buy" else float(
                TradeType.SELL.value),
            "amount": trade_data["v"],
            "price": trade_data["p"]
        }
        trade_message: Optional[OrderBookMessage] = OrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content=message_content,
            timestamp=timestamp)

        message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        diff_data: [str, Any] = raw_message["data"]
        timestamp: float = self._time()
        update_id: int = diff_data["sequenceEnd"]

        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=diff_data["symbol"])

        order_book_message_content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "first_update_id": diff_data["sequenceStart"],
            "bids": diff_data["changes"]["bids"],
            "asks": diff_data["changes"]["asks"],
        }
        diff_message: OrderBookMessage = OrderBookMessage(
            OrderBookMessageType.DIFF,
            order_book_message_content,
            timestamp)

        message_queue.put_nowait(diff_message)

    aasync def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.

        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                trades_payload = {
                    "action": "sub",
                    "subscriptions": [
                        {
                            "channel": CONSTANTS.TRADES_ENDPOINT_NAME,
                            "market": symbol
                        }
                    ],
                    "id": CONSTANTS.ID
                }
                subscribe_trade_request: WSJSONRequest = WSJSONRequest(payload=trades_payload)

                order_book_payload = {
                    "action": "sub",
                    "subscriptions":[
                        {
                            "channel": CONSTANTS.DEPTH_ENDPOINT_NAME,
                            "market": symbol,
                            "depth": CONSTANTS.DEPTH
                        }
                    ],
                    "id": CONSTANTS.ID
                }
                subscribe_orderbook_request: WSJSONRequest = WSJSONRequest(payload=order_book_payload)

                await ws.send(subscribe_trade_request)
                await ws.send(subscribe_orderbook_request)

                self.logger().info("Subscribed to public order book, trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Unexpected error occurred subscribing to order book data streams.")
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = ""
        event_channel = event_message.get("c")
        if event_channel == CONSTANTS.TRADE_EVENT_TYPE:
            channel = self._trade_messages_queue_key
        elif event_channel == CONSTANTS.DIFF_EVENT_TYPE and event_message["action"] == "update":
            channel = self._diff_messages_queue_key
        elif event_channel == CONSTANTS.DIFF_EVENT_TYPE and event_message["action"] == "snapshot":
            channel = self._snapshot_messages_queue_key

        return channel

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        while True:
            try:
                await super()._process_websocket_messages(websocket_assistant=websocket_assistant)
            except asyncio.TimeoutError:
                ping_request = WSPlainTextRequest(payload="ping")
                await websocket_assistant.send(request=ping_request)

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WS_URL,
                         ping_timeout=CONSTANTS.PING_TIMEOUT)
        return ws
