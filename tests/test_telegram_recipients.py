import asyncio

import pytest

from crypto_flow_bot.config import Config, NotifierCfg
from crypto_flow_bot.notify.telegram import (
    TelegramDeliveryError,
    TelegramNotifier,
    format_greeting,
    normalize_chat_ids,
)


class _DummyResponse:
    def __init__(self, status_code: int = 200, text: str = "ok") -> None:
        self.status_code = status_code
        self.text = text

    def json(self) -> dict[str, object]:
        return {"result": []}


class _DummyHTTP:
    def __init__(self, responses_by_chat_id: dict[str, _DummyResponse] | None = None) -> None:
        self.responses_by_chat_id = responses_by_chat_id or {}
        self.posts: list[dict[str, object]] = []

    async def post(self, url: str, json: dict[str, object]) -> _DummyResponse:
        self.posts.append(json)
        return self.responses_by_chat_id.get(str(json["chat_id"]), _DummyResponse())


class _ExplodingHTTP(_DummyHTTP):
    async def post(self, url: str, json: dict[str, object]) -> _DummyResponse:
        self.posts.append(json)
        if json["chat_id"] == "222":
            raise TimeoutError("network timeout")
        return _DummyResponse()


def test_normalize_chat_ids_accepts_railway_separator_variants_and_dedupes():
    assert normalize_chat_ids([" 111,222\n333", "222; @channel  -100444 "]) == [
        "111",
        "222",
        "333",
        "@channel",
        "-100444",
    ]


def test_notifier_broadcasts_to_every_normalized_chat_id():
    http = _DummyHTTP()
    notifier = TelegramNotifier(
        bot_token="123456:ABCDEF",
        chat_ids=["111,222\n333", "222;@channel"],
        http=http,  # type: ignore[arg-type]
    )

    asyncio.run(notifier.send("hello"))

    assert notifier.chat_ids == ["111", "222", "333", "@channel"]
    assert [post["chat_id"] for post in http.posts] == ["111", "222", "333", "@channel"]


def test_send_attempts_all_chats_then_raises_for_partial_http_failure():
    http = _DummyHTTP({"222": _DummyResponse(status_code=403, text="Forbidden")})
    notifier = TelegramNotifier(
        bot_token="123456:ABCDEF",
        chat_ids=["111", "222", "333"],
        http=http,  # type: ignore[arg-type]
    )

    with pytest.raises(TelegramDeliveryError) as exc:
        asyncio.run(notifier.send("signal"))

    assert [post["chat_id"] for post in http.posts] == ["111", "222", "333"]
    assert exc.value.success_count == 2
    assert [failure.chat_id for failure in exc.value.failures] == ["222"]


def test_send_attempts_all_chats_then_raises_for_partial_network_error():
    http = _ExplodingHTTP()
    notifier = TelegramNotifier(
        bot_token="123456:ABCDEF",
        chat_ids=["111", "222", "333"],
        http=http,  # type: ignore[arg-type]
    )

    with pytest.raises(TelegramDeliveryError) as exc:
        asyncio.run(notifier.send("signal"))

    assert [post["chat_id"] for post in http.posts] == ["111", "222", "333"]
    assert exc.value.success_count == 2
    assert [failure.chat_id for failure in exc.value.failures] == ["222"]


def test_start_greeting_marks_unsubscribed_chats_so_broadcast_config_is_obvious():
    cfg = Config(symbols=["BTCUSDT"], notifier=NotifierCfg(pretty_names={"BTCUSDT": "BTC"}))

    text = format_greeting(cfg, subscribed=False, chat_id="222")

    assert "not in TELEGRAM_CHAT_IDS" in text
    assert "<code>222</code>" in text
