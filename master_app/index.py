from master_app.router import app
from services.router_class import Router

main_app_master = Router()
main_app_master.add_routes_from_other_routers(app)

if __name__ == "__main__":
    main_app_master.run_app(host="localhost", port=8765)
