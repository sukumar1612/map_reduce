from worker_app.router import Router, app

main_app = Router()
main_app.add_routes_from_other_routers(app)

if __name__ == "__main__":
    main_app.run_app(host="localhost", port=8765)
