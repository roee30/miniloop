# import asyncio
import logging
import socket

import miniloop as asyncio

log = logging.getLogger("loop.server")


async def handle_client(client):
    loop = asyncio.get_event_loop()
    request = None
    while request != "quit":
        request = (await loop.sock_recv(client, 255)).decode("utf8")
        response = str(eval(request)) + "\n"
        await loop.sock_sendall(client, response.encode("utf8"))
    log.info("goodbye")
    client.close()
    return "client handling complete"


async def divide(a, b):
    return a / b


async def run_server():
    # check that the loop handles tasks that don't yield futures
    result = await divide(1, 2)
    log.info("got: %s", result)
    try:
        await divide(1, 0)
    except ZeroDivisionError:
        log.info("caught expected exception")
    else:
        assert False, "unreachable"

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("localhost", 8888))
    server.listen(8)
    server.setblocking(False)

    loop = asyncio.get_event_loop()

    while True:
        log.debug("accepting")
        result = await loop.sock_accept(server)
        assert result, result
        client, _address = result
        log.debug("accepted")
        log.debug("handling client")
        loop.create_task(handle_client(client))


asyncio.run(run_server())
