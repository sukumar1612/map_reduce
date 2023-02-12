import asyncio

import websockets

from worker_app.router import Router, app

main_app = Router()
main_app.add_routes_from_other_routers(app)


async def main_app_handler():
    async with websockets.serve(main_app.handler, "localhost", 8765):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main_app_handler())
