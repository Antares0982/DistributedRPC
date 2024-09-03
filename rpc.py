import asyncio
from typing import TYPE_CHECKING, Any, Awaitable, Callable

import msgpack  # type: ignore

from pika_interface import listen_to

if TYPE_CHECKING:
    from pika_interface import AbstractIncomingMessage


RpcHandler = Callable[["RpcContent"], Awaitable[Any]]
RpcChecker = Callable[["RpcContent"], Awaitable[bool]] | Callable[["RpcContent"], bool] | None


class RpcHandleManager:
    def __init__(self, client_name: str, host: str | None = None, port: int | None = None, login: str | None = None, password: str | None = None, virtualhost: str | None = None, ssl: bool | None = None):
        self.client_name = client_name
        #
        self.host = host
        self.port = port
        self.login = login
        self.password = password
        self.virtualhost = virtualhost
        self.ssl = ssl
        #
        self.handlers: dict[str, tuple[RpcChecker, RpcHandler]] = dict()
        self.canceller: Callable[[], Awaitable[Any]] | None = None

    def register_handler(self, method_name: str, handler: RpcHandler, checker: RpcChecker = None):
        if method_name in self.handlers:
            raise ValueError("Method already registered")
        self.handlers[method_name] = (checker, handler)

    def _to_connection_kw(self):
        connection_kw = dict()
        for k in ["host", "port", "login", "password", "virtualhost", "ssl"]:
            val = getattr(self, k, None)
            if val is not None:
                connection_kw[k] = val
        return connection_kw

    def connect(self, loop: asyncio.AbstractEventLoop):
        if self.canceller is not None:
            raise RuntimeError("Already connected")
        self.canceller = listen_to(loop, f"rpc.{self.client_name}", self._rpc_handle0, **self._to_connection_kw())

    async def rpc_handler(self, rpc: "RpcContent"):
        v = self.handlers.get(rpc.method)
        if v is not None:
            # checker is None => always pass
            # otherwise, checker is a function that returns a boolean, or an awaitable that returns a boolean
            checker, handler = v
            passed = checker is None
            if checker is not None:
                _passed = checker(rpc)
                if isinstance(_passed, bool):
                    passed = _passed
                else:
                    passed = await _passed
            if passed:
                await handler(rpc)
        else:
            raise ValueError("No handler for method", rpc.method)

    async def _rpc_handle0(self, message: "AbstractIncomingMessage"):
        await RpcHandleManager.rpc_handler_static(self.rpc_handler, message)

    async def cancel(self):
        if self.canceller:
            await self.canceller()
            self.canceller = None

    @staticmethod
    async def rpc_handler_static(true_rpc_handler: Callable[["RpcContent"], Awaitable[Any]], message: "AbstractIncomingMessage"):
        try:
            obj = RpcContent.deserialize(message.body)
        except Exception as e:
            raise ValueError("Invalid message", message.body) from e
        try:
            await true_rpc_handler(obj)
        except Exception as e:
            raise ValueError("Error when calling rpc_handler") from e


class RpcContent:
    def __init__(self, sender: str, method: str, args: list[str] | None = None, kwargs: dict[str, Any] | None = None):
        self.sender = sender
        self.method = method
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        # do type check
        self._check()

    def _check(self):
        if not isinstance(self.sender, str):
            raise ValueError("sender must be a string")
        if not isinstance(self.method, str):
            raise ValueError("method must be a string")
        if not isinstance(self.args, list):
            raise ValueError("args must be a list")
        if not isinstance(self.kwargs, dict):
            raise ValueError("kwargs must be a dict")
        for k in self.kwargs:
            if not isinstance(k, str):
                raise ValueError("kwargs keys must be strings")

    def serialize(self):
        pack = (self.sender, self.method, self.args if self.args is not None else [], self.kwargs if self.kwargs is not None else {})
        return msgpack.packb(pack, use_bin_type=True)

    @classmethod
    def deserialize(cls, data: bytes):
        sender, method, args, kwargs = msgpack.unpackb(data, raw=False)
        return cls(sender, method, args, kwargs)
