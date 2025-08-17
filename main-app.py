from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
import time
import os
import logging
import base64
import asyncio
import aiohttp
import requests
import csv
from ftplib import FTP
from retrying import retry
from typing import Optional
import uvicorn
import io

# Configure logging
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Product Data Processor", version="2.0.0")

# ----------- Utility Functions -----------

def generate_basic_auth_token(username: str, password: str):
    return base64.b64encode(f"{username}:{password}".encode('utf-8')).decode('utf-8')

@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
async def fetch_data(endpoint, session, page, api_token, supplier):
    params = {"page": page, "size": 500, "suppliers": supplier}
    headers = {"Authorization": f"Bearer {api_token}"}

    async with session.get(endpoint, params=params, headers=headers) as response:
        response.raise_for_status()
        return await response.json()

async def fetch_all_data(endpoint, api_token, slack_webhook_url, supplier):
    all_data = []
    start_time = time.time()

    async with aiohttp.ClientSession() as session:
        page = 0
        while True:
            try:
              # logging.info(f"Fetching data for page {page} with supplier {supplier}...")
                data = await fetch_data(endpoint, session, page, api_token, supplier)
                if not data:
                    break
                all_data.append(data)
                page += 1
            except Exception as e:
               # logging.error(f"Error fetching data on page {page}: {e}")
                break

    duration = time.time() - start_time
    message = f"Total products processed: {len(all_data)} for supplier {supplier}. Duration: {duration:.2f} seconds."
    send_to_slack(slack_webhook_url, message)
    return all_data

def generate_token(client_id, client_secret):
    token_url = f"https://api.buyogo.com/api/oauth2/token/60"
    params = {
        "client_id": client_id,
        "client_secret": client_secret
    }

    response = requests.get(token_url, params=params)
    if response.status_code == 200:
        return response.json()["access_token"]
    else:
        raise Exception(f"Failed to generate token. Status code: {response.status_code}, Error: {response.text}")

def write_to_csv(data, filename):
    if not data:
        return False

    csv_data = [["GTIN","supplier_price"]]
    for product_list in data:
        for item in product_list:
            suppliers_data = item.get("suppliers", [])
            # supplier = ""
            # supplier_price = 0.0

            if suppliers_data and isinstance(suppliers_data, list):
                supplier_info = suppliers_data[0]
                # supplier = supplier_info.get("name", "")
                supplier_price = supplier_info.get("price")

            for variant in item.get("variants_list", []):
                article_ean = variant.get("article_ean")
                name = variant.get("name", {}).get("GERMAN", "")
                base_price_exclusive_tax = variant.get("base_price_exclusive_tax")

                if article_ean:
                    csv_data.append([article_ean, supplier_price])

    with open(filename, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(csv_data)
    return True

def upload_to_ftp(local_file_path, ftp_host, ftp_port, ftp_user, ftp_pass, remote_file_path):
    try:
        with FTP() as ftp:
            ftp.connect(ftp_host, ftp_port, timeout=10)
            ftp.login(user=ftp_user, passwd=ftp_pass)

            # Navigate to the directory
            remote_dir, remote_file = os.path.split(remote_file_path)
            if remote_dir:
                ftp.cwd(remote_dir)

            with open(local_file_path, 'rb') as file:
                ftp.storbinary(f'STOR {remote_file}', file)

           # logging.info(f"✅ File uploaded to FTP: {remote_file_path}")
            return True
    except Exception as e:
      #  logging.error(f"❌ FTP upload failed: {str(e)}")
        return False

def send_to_slack(webhook_url, message):
    try:
        payload = {"text": message}
        response = requests.post(webhook_url, json=payload)
        if response.status_code != 200:
          #  logging.error(f"Failed to send Slack message: {response.text}")
            return False
        return True
    except Exception as e:
       # logging.error(f"Error sending Slack message: {e}")
        return False

# ----------- CONFIGURATION -----------

ENDPOINT = "https://api.getlynks.com/products"


CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/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"

# ----------- FASTAPI ENDPOINTS -----------

@app.get("/")
async def root():
    return {"message": "Product Data Processor API", "version": "2.0.0"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": time.time()}

@app.post("/process-products")
async def process_products(
    supplier: str = Query(..., description="Supplier name to filter products"),
    ftp_host: str = Query(..., description="FTP host"),
    ftp_port: int = Query(22, description="FTP port, default is 22"),
    ftp_user: str = Query(..., description="FTP username"),
    ftp_pass: str = Query(..., description="FTP password"),
    remote_path: str = Query(..., description="Remote FTP path to upload the file, e.g. /upload/myfile.csv")
):
    try:
        filename = f"{supplier}_prices.csv"

        # Generate token
    #  logging.info("🔐 Generating API token...")
        api_token = generate_token(CLIENT_ID, CLIENT_SECRET)

        # Fetch product data
      #  logging.info(f"📦 Fetching all product data for supplier: {supplier}...")
        all_data = await fetch_all_data(ENDPOINT, api_token, SLACK_WEBHOOK_URL, supplier)
        if not all_data:
            raise HTTPException(status_code=404, detail=f"No data found for supplier: {supplier}")

        # Write to CSV
       # logging.info("📄 Writing data to CSV file...")
        if not write_to_csv(all_data, filename):
            raise HTTPException(status_code=500, detail="Failed to create CSV file")

        # Upload to FTP
      #  logging.info("📤 Uploading file to FTP...")
        if not upload_to_ftp(filename, ftp_host, ftp_port, ftp_user, ftp_pass, remote_path):
            raise HTTPException(status_code=500, detail="Failed to upload file to FTP")

        # Cleanup
        try:
            os.remove(filename)
        #    logging.info(f"🗑️ Removed local file: {filename}")
        except Exception as e:
         #   logging.warning(f"Could not delete local file: {e}")

        # Notify
        success_message = f"✅ Successfully processed products for supplier '{supplier}'. File uploaded to FTP: {remote_path}"
        send_to_slack(SLACK_WEBHOOK_URL, success_message)

        return {
            "message": "Prices processed and updated successfully",
            "supplier": supplier,
            "filename": filename,
            "total_pages": len(all_data),
            "ftp_destination": remote_path,
            "status": "completed"
        }

    except HTTPException:
        raise
    except Exception as e:
        error_message = f"❌ Error processing products for supplier '{supplier}': {str(e)}"
        send_to_slack(SLACK_WEBHOOK_URL, error_message)
       # logging.error(error_message)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/fetch-products")
async def fetch_products(
    supplier: str = Query(..., description="Supplier name to filter products"),
    ftp_host: str = Query(..., description="FTP host"),
    ftp_port: int = Query(22, description="FTP port, default is 22"),
    ftp_user: str = Query(..., description="FTP username"),
    ftp_pass: str = Query(..., description="FTP password"),
    remote_path: str = Query(..., description="Remote FTP path to upload the file, e.g. /upload/myfile.csv")
):
    try:
        filename = f"{supplier}_products.csv"

        # Generate token
        api_token = generate_token(CLIENT_ID, CLIENT_SECRET)

        # Fetch product data
        logging.info(f"📦 Fetching all product data for supplier: {supplier}...")
        all_data = await fetch_all_data(ENDPOINT, api_token, SLACK_WEBHOOK_URL, supplier)
        if not all_data:
            raise HTTPException(status_code=404, detail=f"No data found for supplier: {supplier}")

        # Write to CSV (with more product info)

        csv_data = [["GTIN", "SKU","product_name", "Supplier", "Brand", "Tax", "Category", "Image"]]
        for product_list in all_data:
            for item in product_list:
                Brand = item.get("brand_name")
                Tax = item.get("tax_in_percentage")
                suppliers_data = item.get("suppliers", [])
                if suppliers_data and isinstance(suppliers_data, list):
                    supplier_info = suppliers_data[0]
                    supplier_name = supplier_info.get("name")
                for variant in item.get("variants_list", []):
                    article_ean = variant.get("article_ean")
                    seller_sku_id = variant.get("seller_sku_id")
                    name = variant.get("name", {}).get("GERMAN", "")
                    category_list = variant.get("category_tree", {}).get("GERMAN", [])
                    category = category_list[0].strip() if category_list else ""
                    Image = variant.get("multimedia", [{}])[0].get("source_url", "")
                    if article_ean:
                        csv_data.append([article_ean,seller_sku_id, name, supplier_name,Brand, Tax, category, Image])
        with open(filename, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(csv_data)

        # Upload to FTP

        if not upload_to_ftp(filename, ftp_host, ftp_port, ftp_user, ftp_pass, remote_path):
            raise HTTPException(status_code=500, detail="Failed to upload file to FTP")

        # Cleanup
        try:
            os.remove(filename)
            logging.info(f"🗑️ Removed local file: {filename}")
        except Exception as e:
            logging.warning(f"Could not delete local file: {e}")



        return {
            "message": "Products fetched and uploaded successfully",
            "supplier": supplier,
            "filename": filename,
            "total_pages": len(all_data),
            "ftp_destination": remote_path,
            "status": "completed"
        }

    except HTTPException:
        raise
    except Exception as e:
        error_message = f"❌ Error fetching products for supplier '{supplier}': {str(e)}"
        send_to_slack(SLACK_WEBHOOK_URL, error_message)
        logging.error(error_message)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# ----------- RUN SERVER -----------

if __name__ == "__main__":
    uvicorn.run(
        "main-app:app",  # Replace with your filename if not named main.py
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
