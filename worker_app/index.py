from services.router_class import Router
from worker_app.router import app

main_app_worker = Router(node_type="worker")
main_app_worker.add_routes_from_other_routers(app)

if __name__ == "__main__":
    main_app_worker.run_app(host="localhost", port=8763)
