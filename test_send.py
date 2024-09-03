import asyncio

import cfg
from pika_interface import create_sustained_connection, send_message
from rpc import RpcContent


async def test_send():
    data = RpcContent("pi", "hello").serialize()
    await send_message("rpc.nixos", data)


def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    connection_kw = dict()
    for k in ["host", "port", "login", "password", "virtualhost", "ssl"]:
        val = getattr(cfg, k, None)
        if val is not None:
            connection_kw[k] = val
    loop.run_until_complete(create_sustained_connection(**connection_kw))
    loop.run_until_complete(test_send())


if __name__ == "__main__":
    main()
