from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse
import bayside_scraper
import uvicorn
from typing import Optional
from pydantic import BaseModel
import os
from datetime import datetime

app = FastAPI()

class ScraperStatus:
    def __init__(self):
        self.is_running = False
        self.last_run = None
        self.total_listings = 0
        self.error_message = None

scraper_status = ScraperStatus()

async def run_scraper(max_listings: Optional[int] = None):
    try:
        scraper_status.is_running = True
        scraper_status.error_message = None
        
        # Modify MAX_LISTINGS if specified
        if max_listings:
            bayside_scraper.MAX_LISTINGS = max_listings
        
        # Run the scraper
        bayside_scraper.main()
        
        scraper_status.last_run = datetime.now()
        scraper_status.total_listings = len(bayside_scraper.all_listings_data)
        
    except Exception as e:
        scraper_status.error_message = str(e)
    finally:
        scraper_status.is_running = False

@app.post("/scrape")
async def start_scraper(background_tasks: BackgroundTasks, max_listings: Optional[int] = None):
    if scraper_status.is_running:
        return JSONResponse(
            status_code=409,
            content={"message": "Scraper is already running"}
        )
    
    background_tasks.add_task(run_scraper, max_listings)
    return {"message": "Scraper started successfully"}

@app.get("/status")
async def get_status():
    return {
        "is_running": scraper_status.is_running,
        "last_run": scraper_status.last_run,
        "total_listings": scraper_status.total_listings,
        "error_message": scraper_status.error_message
    }

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)