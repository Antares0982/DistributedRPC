import asyncio
import signal
import sys

from rpc import RpcContent, RpcHandleManager


async def hello_handler(obj: RpcContent):
    assert obj.method == "hello"
    print("hello, world!")


def _exit_func(*args):
    print("exiting...")
    sys.exit(0)


def main():
    signal.signal(signal.SIGINT, _exit_func)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    import cfg
    connection_kw = dict()
    for k in ["host", "port", "login", "password", "virtualhost", "ssl"]:
        val = getattr(cfg, k, None)
        if val is not None:
            connection_kw[k] = val
    manager = RpcHandleManager(cfg.CLIENT_NAME, **connection_kw)
    manager.register_handler("hello", hello_handler)
    manager.connect(loop)
    loop.run_forever()


if __name__ == "__main__":
    main()
