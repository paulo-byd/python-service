from fastapi import FastAPI, Form, Query
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
    claim_no: str = Query(default=None),
    vin: str = Query(default=None),
    dealer_code: str = Query(default=None),

    #Status Params
    attachment_status: str = Query(default=None),
    audit_status: str = Query(default=None),

    #Date Params
    bgate_last_processed_date_start: str = Query(default=None),
    bgate_last_processed_date_end: str = Query(default=None)
   
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

        claim_list = claim_no.replace(' ', '').split(',')
        placeholders = ", ".join([f':claim_no{i}' for i in range(len(claim_list))])

        conditions.append(f"cs.CLAIM_NO IN ({placeholders})")
        for i, claim in enumerate(claim_list):
            params[f'claim_no{i}'] = claim

    if vin:

        vin_list = vin.replace(' ', '').split(',')
        placeholders = ", ".join([f':vin{i}' for i in range(len(vin_list))])

        conditions.append(f"cs.VIN IN ({placeholders})")
        for i, v in enumerate(vin_list):
            params[f'vin{i}'] = v

    if dealer_code:  # Example: "BYDAMEBR0015W, BYDAMEBR0020W,BYDAMEBR0030W"
         
        dealer_code_list = [code.strip() for code in dealer_code.split(',')] #  ['BYDAMEBR0015W','BYDAMEBR0020W','BYDAMEBR0030W']
        placeholders = ", ".join([f":dealer_code{i}" for i in range(len(dealer_code_list))]) # (:dealer_code_0, :dealer_code_1, :dealer_code_2)
        conditions.append(f"cs.DEALER_CODE IN ({placeholders})")

        for i, code in enumerate(dealer_code_list):
            params[f"dealer_code{i}"] = code
    
    if attachment_status:

        attachment_status_list = [status.strip() for status in attachment_status.split(',')] 
        placeholders = ", ".join([f":attachment_status{i}" for i in range(len(attachment_status_list))])
        conditions.append(f"cs.ATTACHMENT_STATUS IN ({placeholders})")

        for i, status in enumerate(attachment_status_list):
            params[f"attachment_status{i}"] = status
    
    if audit_status:

        audit_status_list = audit_status.replace(' ', '').split(',')
        placeholders = ", ".join([f':audit_status{i}' for i in range(len(audit_status_list))])

        conditions.append(f"cs.AUDIT_STATUS IN ({placeholders})")
        for i, status in enumerate(audit_status_list):
            params[f'audit_status{i}'] = status

    # Expected to receive the dates in the format: 'YYYY-MM-DD'
    if bgate_last_processed_date_start or bgate_last_processed_date_end:

        if not bgate_last_processed_date_start:
                bgate_last_processed_date_start = "2002-11-21"

        if not bgate_last_processed_date_end:
            bgate_last_processed_date_end = str(datetime.datetime.today().date())
        
        conditions.append("cs.LAST_MODIFIED_DATE BETWEEN :last_modified_date_start AND :last_modified_date_end")
        
        start_date = datetime.datetime.strptime(bgate_last_processed_date_start, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(bgate_last_processed_date_end, "%Y-%m-%d") + datetime.timedelta(days=1)
        params["last_modified_date_start"] = start_date
        params["last_modified_date_end"] = end_date
    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY cs.LAST_MODIFIED_DATE DESC"

    logger.info(f"Executing query: {query}")
    logger.info(f"Parameters: {params}")

    result = db.get(query, params)

    return result


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Specify the running environment")

    parser.add_argument('--env', type=str, required=True, help='Select one of the environments: local, uat or prod')
    parser.add_argument('--reload', type=str, required=False, help='Enables hot reloading for uvicorn development')


    args = parser.parse_args()

    env_mode = 'local'

    logger.info(f"Chosen environment: {str(env_mode).upper()}")

    db = Database(env_mode)
    db.test_connection()

    uvicorn.run(app, host="0.0.0.0", port=PORT,reload=args.reload)
