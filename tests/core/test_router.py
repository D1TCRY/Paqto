import pytest

from paqto.core.errors import MessageRoutingError
from paqto.core.message import Message
from paqto.core.router import MessageRouter


@pytest.mark.asyncio
async def test_handler_failures_are_normalized() -> None:
    router = MessageRouter()

    @router.on("event")
    def fail(message: Message) -> None:
        raise ValueError("handler failed")

    with pytest.raises(MessageRoutingError) as captured:
        await router.dispatch(Message(payload=None, type="event"))

    assert isinstance(captured.value.__cause__, ValueError)
