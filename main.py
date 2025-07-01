import os
from .et import create_database
from .et import load
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    schema = "public"

    # ======= Create database ========
    db_name = create_database()

    # Connection string for mot_vehicles database
    conn_str = f"postgresql://{user}:{password}@{host}:{port}/{db_name}?sslmode=disable"
    
    if not all([user, password, host, port, db_name]):
        logging.error("Missing required database connection parameters.")
        raise ValueError("Database connection parameters incomplete.")

    resources = [
        {"resource_id": "142afde2-6228-49f9-8a29-9b6c3a0cbe40", "table_name": "mot_submodels"},
        {"resource_id": "5e87a7a1-2f6f-41c1-8aec-7216d52a6cf6", "table_name": "mot_amounts"},
        {"resource_id": "bb2355dc-9ec7-4f06-9c3f-3344672171da", "table_name": "mot_car_history_owners"},
        {"resource_id": "56063a99-8a3e-4ff4-912e-5966c0279bad", "table_name": "mot_car_history_physical"},
        {"resource_id": "053cea08-09bc-40ec-8f7a-156f0677aff3", "table_name": "mot_imported_private"},
        {"resource_id": "c967097c-3c74-4adf-a732-0fdd2fda56d9", "table_name": "mot_monthly_on_road_no_degemcode"},
        {"resource_id": "602ac32d-19c0-4b41-88e0-e3ce8a7e80b7", "table_name": "mot_monthly_on_road"},
        {"resource_id": "0866573c-40cd-4ca8-91d2-9dd2d7a492e5", "table_name": "mot_plates_com_and_private"},
        {"resource_id": "053cea08-09bc-40ec-8f7a-156f0677aff3", "table_name": "mot_plates_private"},
        {"resource_id": "39f455bf-6db0-4926-859d-017f34eacbcb", "table_name": "mot_price_new"},
        {"resource_id": "36bf1404-0be4-49d2-82dc-2f1ead4a8b93", "table_name": "mot_recall_required"},
        {"resource_id": "2c33523f-87aa-44ec-a736-edbb0a82975e", "table_name": "mot_recalls"},
        {"resource_id": "83bfb278-7be1-4dab-ae2d-40125a923da1", "table_name": "mot_safety_discount_ind"},
        {"resource_id": "cf29862d-ca25-4691-84f6-1be60dcb4a1e", "table_name": "mot_public_transport"},
        {"resource_id": "7cb2bd95-bf2e-49b6-aea1-fcb5ff6f0473", "table_name": "pollution_filter"},
        {"resource_id": "f6efe89a-fb3d-43a4-bb61-9bf12a9b9099", "table_name": "mot_no_yearly_inspection"},
        {"resource_id": "6f6acd03-f351-4a8f-8ecf-df792f4f573a", "table_name": "mot_inactive_no_degemcode"},
        {"resource_id": "851ecab1-0622-4dbe-a6c7-f950cf82abf9", "table_name": "mot_taken_off_2016_today"},
        {"resource_id": "4e6b9724-4c1e-43f0-909a-154d4cc4e046", "table_name": "mot_taken_off_2010_2016"},
        {"resource_id": "ec8cbc34-72e1-4b69-9c48-22821ba0bd6c", "table_name": "mot_taken_off_2000_2009"}
    ]

    for resource in resources:
        logging.info(f"Processing resource {resource['resource_id']} into table {resource['table_name']}")
        try:
            load(resource['resource_id'], resource['table_name'], conn_str, schema)
        except Exception as e:
            logging.error(f"Failed to process resource {resource['resource_id']}: {e}")
            continue

if __name__ == "__main__":
    main()
