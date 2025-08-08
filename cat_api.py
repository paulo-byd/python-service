from fastapi import FastAPI, Form
from contextlib import asynccontextmanager
import logging
import argparse
import uvicorn
import oracledb
from sqlalchemy import Integer, create_engine, text
import datetime

# Environment-specific database configurations
DB_SETTINGS = {

    "local" : {
        "user": "temp_dms",
        "password": "0<wS16q:F}|o.+",
        "ip": "10.42.253.86:1092",
        "service_name": "dms19g_pdb1"
    },

    "uat" : {
        "user": "temp_dms",
        "password": "0<wS16q:F}|o.+",
        "ip": "10.42.253.92:1521",
        "service_name": "dms19g_pdb1"
    },

    "prod" : {
        "user": "prod_dms",
        "password": "1gHH16Dkjqyj:>D",
        "ip": "10.42.253.92:1521",
        "service_name": "dms19g_pdb1"
    }
}

PORT = 8777

class Database:
    def __init__(self, env):   
        config =  DB_SETTINGS[env]
        dsn = f"oracle+oracledb://{config['user']}:{config['password']}@{config['ip']}/?service_name={config['service_name']}"
        self.engine = create_engine(dsn, pool_pre_ping=True)

    def test_connection(self):
        logger.info("Testing Database connection...")
        try:
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1 FROM DUAL"))
                logger.info("Database connected!")
            return True
        except Exception as e:
            logger.info(f"Database Connection Failed. Error: {e}")
            return False

    def get(self, query, params=None):
        with self.engine.connect() as connection:
            result = connection.execute(text(query), parameters=params or {}).mappings().fetchall()
            return result

    def dispose(self):
        """Closes all connections in the connection pool."""
        self.engine.dispose()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Log API start and end
@asynccontextmanager
async def lifespan(app: FastAPI):
    #Before the API starts
    logger.info("Claim and Attachments API starting up...")
    logger.info("")
    logger.info(f"Server running on port {PORT}")
    yield
    #After the API shutdown
    logger.info("Shuting down Claim and Attachments API")

app = FastAPI(
    title= "Claims and Attachments API (索赔和附件API)",
    version= "1.0.0",
    description= "API for getting Claims and Attachments form BYD Brazil Systems Team"
)

@app.get(path="/")
async def root():
    return {
        "message" : "Welcome to our cat 😸 API!",
        "endpoints" : {
            "claims" : "/claims",
            "attachments" : "/attachments"
        }
    }

@app.get("/claims")
async def get_claims_data(
    claim_no: str = Form(default=None),
    vin: str = Form(default=None),
    dealer_code: str = Form(default=None),

    #Status Params
    attachment_status: str = Form(default=None),
    audit_status: str = Form(default=None),

    #Date Params
    bgate_last_processed_date_start: str = Form(default=None),
    bgate_last_processed_date_end: str = Form(default=None)
   
):
    """Returns Claims Status data"""

    query = """SELECT 
            CLAIM_NO,VIN,DEALER_CODE,DEALER_NAME,REPORT_DATE,GROSS_CREDIT,LABOUR_AMOUNT_DMS,PART_AMOUNT_DMS,TOTAL_FILES_COUNT,DOWNLOADED_FILES_COUNT,LAST_DMS_UPDATE_DATE,AUDITING_DATE,ATTACHMENT_STATUS,AUDIT_STATUS,CREATED_BY,CREATED_DATE,LAST_MODIFIED_BY,LAST_MODIFIED_DATE,LABOUR_AMOUNT_PROCESSING,PART_AMOUNT_PROCESSING
            FROM CLAIM_STATUS cs
    """

    # Check each parameter 
    conditions = []
    params = {}

    if claim_no: 
        conditions.append("cs.CLAIM_NO IN (:claim_no)")
        params["claim_no"] = claim_no

    if vin:
        conditions.append("cs.VIN IN (:vin)")
        params["vin"] = ','.join(vin)
    print(f"DEALER CODE: {dealer_code}")
    if dealer_code:
        logger.info(f"Received dealer_code: {dealer_code}")
        conditions.append("cs.DEALER_CODE IN (:dealer_code)")
        params["dealer_code"] = dealer_code
    
    if attachment_status:
        conditions.append("cs.ATTACHMENT_STATUS IN (:attachment_status)")
        params["attachment_status"] = attachment_status
    
    if audit_status:
        conditions.append("cs.AUDIT_STATUS IN (:audit_status)")
        params["audit_status"] = audit_status

    # Expected to receive the dates in the format: 'YYYY-MM-DD'

    if bgate_last_processed_date_start or bgate_last_processed_date_end:

        if not bgate_last_processed_date_start:
                bgate_last_processed_date_start = "2002-11-21"

        if not bgate_last_processed_date_end:
            bgate_last_processed_date_end = datetime.datetime.today().date()
        
        conditions.append("cs.LAST_MODIFIED_DATE BETWEEN :last_modified_date_start AND :last_modified_date_end")
        
        start_date = datetime.datetime.strptime(bgate_last_processed_date_start, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(bgate_last_processed_date_end, "%Y-%m-%d") + datetime.timedelta(days=1)
        params["last_modified_date_start"] = start_date
        params["last_modified_date_end"] = end_date
    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY cs.LAST_MODIFIED_DATE DESC"

    query += " FETCH FIRST 20 ROWS ONLY"

    logger.info(f"Executing query: {query}")
    logger.info(f"Parameters: {params}")

    result = db.get(query, params)

    return result


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Specify the running environment")

    parser.add_argument('--env', type=str, required=True, help='Select one of the environments: local, uat or prod')

    args = parser.parse_args()

    env_mode = 'local'

    logger.info(f"Chosen environment: {str(env_mode).upper()}")

    db = Database(env_mode)
    db.test_connection()

    uvicorn.run(app, host="0.0.0.0", port=PORT)
