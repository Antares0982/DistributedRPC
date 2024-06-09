import json as stdjson


try:
    import orjson as json
    _USE_ORJSON = True
except ImportError:
    json = stdjson
    _USE_ORJSON = False

import asyncio
import signal
import sys
from typing import TYPE_CHECKING, Any, Awaitable, Callable

import cfg
from pika_interface import listen_to


if TYPE_CHECKING:
    from pika_interface import AbstractIncomingMessage

canceller = None

connection_kw = dict()


def add_key(name):
    val = getattr(cfg, name, None)
    if val is not None:
        connection_kw[name] = val


add_key("host")
add_key("port")
add_key("login")
add_key("password")
add_key("virtualhost")
add_key("ssl")


class RpcContent(dict):
    method: str
    args: list[str]

    def __getattr__(self, attr):
        return self.get(attr)

    def __setattr__(self, attr, value):
        self[attr] = value


# async def rpc_call(target_host: str, target_method: str, args: list[str]):
#     rpc_body = {
#         'method': target_method,
#         'args': args
#     }
#     content: str | bytes = json.dumps(rpc_body)
#     print(f"rpc.{target_host}.{target_method}")
#     await send_message(f"rpc.{target_host}.{target_method}", content)


async def rpc_handler(target_method: str, args: list[str]):
    if target_method == 'hello':
        print("hello, world!")


async def pika_rpc_handler_client(message: "AbstractIncomingMessage"):
    obj: RpcContent = stdjson.loads(message.body, object_hook=RpcContent)
    await rpc_handler(obj.method, obj.args)


async def pika_rpc_handler_server(message: "AbstractIncomingMessage"):
    pass


def _exit_func(*args):
    print("exiting...")
    sys.exit(0)


def main(handler: Callable[["AbstractIncomingMessage"], Awaitable[Any]]):
    signal.signal(signal.SIGINT, _exit_func)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    global canceller
    # loop.run_until_complete(create_sustained_connection(**connection_kw))
    canceller = listen_to(loop, f"rpc.{cfg.CLIENT_NAME}", handler, **connection_kw)
    loop.run_forever()


if __name__ == "__main__":
    if cfg.SERVER:
        main(pika_rpc_handler_server)
    else:
        main(pika_rpc_handler_client)
