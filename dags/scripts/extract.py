# import libraries
import json
import pytz
from datetime import datetime
import logging
import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# define function
LOCATIONS = [(63,125), (53,124), (66,120), (86,86)] 
KST = pytz.timezone("Asia/Seoul")
now = datetime.now(KST).replace(microsecond=0, second=0, minute=0)
base_date = now.strftime("%Y%m%d")
base_time = now.strftime("%H%M")


# logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fetch_weather_data(ti):
    try:
        logger.info("Step 1: fetching data from multiple weather API endpoints")

        # setup a session with retry logic
        # Set up a session with retry logic
        session = requests.Session()
        retry_strategy = Retry(
            total=3,  # Retry up to 3 times
            backoff_factor=1,  # Wait 1, 2, 4 seconds between retries
            status_forcelist=[500, 502, 503, 504],  # Retry on server errors
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        
        all_items=[]
        # write code here
        for nx, ny in LOCATIONS:
            params = {
                    'serviceKey': 'RhXURHVaUAqX9AS4dKYbnvOnegy8sGL1hWqwmUYZbv4QdBuStJWpVTcXUdquDSPp/vsHR1ItrM3JqEr92xP4jw==',
                    'dataType': 'JSON',
                    'base_date': base_date,
                    'base_time': base_time,
                    'nx': nx,
                    'ny': ny
                    }
            try:
                response = session.get(
                    'http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst',
                    params=params,
                    timeout=30 # Increased from 10 to 30 seconds
                )
                response.raise_for_status()
                data = response.json()
                # extract items from response
                items = data['response']['body']['items']['item']
                for item in items:
                    all_items.append((
                        json.dumps(item),
                        datetime.strptime(item["baseDate"], "%Y%m%d").date(),
                        item["baseTime"],
                        nx,
                        ny
                    ))
            except requests.exceptions.ReadTimeout as e:
                print('hi')
                logger.warning(f"Timeout fetching data for nx={nx}, ny={ny}: {e}")
                continue
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch data for nx={nx}, ny={ny}: {e}")
                raise  # Fail the task for non-timeout errors

        logger.info(f"Fetched {len(all_items)} records from {len(LOCATIONS)} locations")
        # push data to Xcom for next task
        ti.xcom_push(key='weather_data', value=all_items)
        return all_items # optional, for logging/debugging
    
    except Exception as e:
        logger.error(f'Fetch failed : {str(e)}')
        raise
