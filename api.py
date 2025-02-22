from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse
import bayside_scraper
import rpemx_scraper
import uvicorn
from typing import Optional
from pydantic import BaseModel
import os
from datetime import datetime

app = FastAPI()

class ScraperStatus:
    def __init__(self):
        # Backward compatibility
        self.is_running = False
        self.last_run = None
        self.total_listings = 0
        self.error_message = None
        
        # Individual scraper status
        self.bayside_running = False
        self.rpemx_running = False
        self.bayside_last_run = None
        self.rpemx_last_run = None
        self.bayside_total = 0
        self.rpemx_total = 0

scraper_status = ScraperStatus()

async def run_bayside_scraper(max_listings: Optional[int] = None):
    try:
        scraper_status.bayside_running = True
        scraper_status.is_running = True  # Backward compatibility
        scraper_status.error_message = None
        
        if max_listings:
            bayside_scraper.MAX_LISTINGS = max_listings
        
        bayside_scraper.main()
        
        scraper_status.bayside_last_run = datetime.now()
        scraper_status.last_run = scraper_status.bayside_last_run  # Backward compatibility
        scraper_status.bayside_total = len(bayside_scraper.all_listings_data)
        scraper_status.total_listings = scraper_status.bayside_total  # Backward compatibility
        
    except Exception as e:
        scraper_status.error_message = f"Bayside scraper error: {str(e)}"
        raise
    finally:
        scraper_status.bayside_running = False
        scraper_status.is_running = False  # Backward compatibility

async def run_rpemx_scraper():
    try:
        scraper_status.rpemx_running = True
        scraper_status.is_running = True  # Backward compatibility
        
        rpemx_scraper.main()
        
        scraper_status.rpemx_last_run = datetime.now()
        scraper_status.last_run = scraper_status.rpemx_last_run  # Backward compatibility
        scraper_status.rpemx_total = rpemx_scraper.total_listings if hasattr(rpemx_scraper, 'total_listings') else 0
        scraper_status.total_listings += scraper_status.rpemx_total  # Backward compatibility
        
    except Exception as e:
        scraper_status.error_message = f"RPEMX scraper error: {str(e)}"
        raise
    finally:
        scraper_status.rpemx_running = False
        scraper_status.is_running = False  # Backward compatibility

async def run_sequence():
    try:
        # Run bayside first
        await run_bayside_scraper()
        # Only start rpemx if bayside completed successfully
        if not scraper_status.error_message:
            await run_rpemx_scraper()
    except Exception as e:
        scraper_status.error_message = str(e)

# Keep existing endpoint for backward compatibility
@app.post("/scrape")
async def start_scraper(background_tasks: BackgroundTasks, max_listings: Optional[int] = None):
    if scraper_status.is_running:
        return JSONResponse(
            status_code=409,
            content={"message": "Scraper is already running"}
        )
    
    background_tasks.add_task(run_bayside_scraper, max_listings)
    return {"message": "Bayside scraper started successfully"}

# New endpoint for sequential execution
@app.post("/scrape/sequence")
async def start_sequence(background_tasks: BackgroundTasks):
    if scraper_status.is_running or scraper_status.bayside_running or scraper_status.rpemx_running:
        return JSONResponse(
            status_code=409,
            content={"message": "A scraper is already running"}
        )
    
    background_tasks.add_task(run_sequence)
    return {"message": "Sequential scraping started successfully"}

@app.get("/status")
async def get_status():
    return {
        # Backward compatibility
        "is_running": scraper_status.is_running,
        "last_run": scraper_status.last_run,
        "total_listings": scraper_status.total_listings,
        "error_message": scraper_status.error_message,
        # Detailed status
        "sequence_status": {
            "bayside": {
                "running": scraper_status.bayside_running,
                "last_run": scraper_status.bayside_last_run,
                "total_listings": scraper_status.bayside_total
            },
            "rpemx": {
                "running": scraper_status.rpemx_running,
                "last_run": scraper_status.rpemx_last_run,
                "total_listings": scraper_status.rpemx_total
            }
        }
    }

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
