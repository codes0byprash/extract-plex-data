import logging
import requests
from datetime import datetime, timedelta,time
from concurrent.futures import ThreadPoolExecutor, as_completed
from logs.extractionlogger import (
    setup_logging_for_url,
    save_extraction_outputs,
    save_extraction_metadata,
    load_last_extraction_time
    # save_last_extraction_time
)
from src.extractors.accounts.utils import parse_date, build_date_str, generate_year_intervals

def fetch_invoices_for_interval(headers, url, start_date, end_date, extra_digit):
    current_start = start_date
    all_ar_invoices = [] 

    interval_days_list = [365, 180, 90, 60, 30, 14, 7, 2, 1, 0.5, 0.25, 0.125, 0.0625, 0.03125, 0.015625]
    last_successful_index = 0

    while current_start < end_date:
        success = False
        try_range = list(range(max(last_successful_index - 1, 0), len(interval_days_list)))

        for i in try_range:
            days = interval_days_list[i]
            current_end = current_start + timedelta(days=days)
            if current_end > end_date:
                current_end = end_date

            begin_str = build_date_str(current_start, extra_digit)
            end_str = build_date_str(current_end, extra_digit)

            logging.info(f"Requesting from {begin_str} to {end_str} | Interval: {days} days")
            print (f"url check: {url}")

            try:
                response = requests.get(
                    url,
                    headers=headers,
                    params={
                        'modifiedDateBegin': begin_str,
                        'modifiedDateEnd': end_str
                    }
                )
                logging.info(f"API Status: {response.status_code}")

                if response.status_code == 200:
                    data = response.json()
                    if data:
                        all_ar_invoices.extend(data)
                        logging.info(f"Fetched {len(data)} invoices.")
                    else:
                        logging.info("No invoices returned for this interval.")
                    current_start = current_end
                    last_successful_index = i
                    success = True
                    break

                elif response.status_code == 400:
                    try:
                        error_json = response.json()
                        if any(e.get('code') == 'REQUEST_RESOURCE_LIMIT_EXCEEDED' for e in error_json.get('errors', [])):
                            logging.warning(f"Resource limit exceeded for interval {days} days. Trying smaller interval.")
                            continue
                        else:
                            logging.error(f"400 Error not due to resource limit: {error_json}")
                            break
                    except Exception as json_parse_error:
                        logging.error(f"Failed to parse error response JSON: {json_parse_error}")
                        break
                elif response.status_code == 500 and  response.status_code == 503:
                    logging.error(f"Server error {response.status_code}. Retrying after a short delay.")
                    time.sleep(5)
                    continue
            
                else:
                    logging.error(f"Unexpected response {response.status_code}")
                    break
            except Exception as e:
                logging.error(f"Exception in request: {e}")
                break
            


        if not success:
            logging.warning(f"All intervals failed at {current_start}. Skipping 1 day.")
            current_start += timedelta(days=1)

    return all_ar_invoices


def extract_list_ar_invoices(headers, url):
    try:
        outer_logs_dir,log_dir = setup_logging_for_url(url)
        resource_name = url.split('/')[-1] or "default"

        # Determine start and end date for incremental fetch
        now = datetime.utcnow()
        modified_date_end = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "0Z"
        
        # modified_date_begin = "2025-07-01T00:00:00.0000000Z"  # Full load fallback

        last_extracted = load_last_extraction_time(outer_logs_dir,resource_name)
        #if there is no change in the data received from the last extraction, we will not fetch the data again
        if last_extracted:
            modified_date_begin = last_extracted
        else:
            modified_date_begin = "2025-07-01T00:00:00.0000000Z"  # Full load fallback

        modified_parsed_date, extra_digit = parse_date(modified_date_begin)
        end_parsed_date, _ = parse_date(modified_date_end)
        
        intervals = [(modified_parsed_date, end_parsed_date)]
        logging.info(f"Fetching from {modified_date_begin} to {modified_date_end}")
        
        max_workers = min(7,max(1,len(intervals) // 2))

        all_ar_invoices = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_range = {
                executor.submit(fetch_invoices_for_interval, headers, url, start, end, extra_digit): (start, end)
                for start, end in intervals
            }

            for future in as_completed(future_to_range):
                start, end = future_to_range[future]
                try:
                    result = future.result()
                    all_ar_invoices.extend(result)
                except Exception as e:
                    logging.error(f"Failed to fetch for interval {start} to {end}: {e}")
                    
        logging.info(f"Total invoices fetched: {len(all_ar_invoices)}")
        save_extraction_metadata(outer_logs_dir, resource_name, len(all_ar_invoices),modified_date_end)
        logging.info(f"Saving extraction outputs for {resource_name}")
        if not all_ar_invoices:
            logging.warning("No invoices fetched. Exiting without saving outputs.")
            return []
        
        save_extraction_outputs(all_ar_invoices, log_dir, resource_name)
        return all_ar_invoices

    except Exception as e:
        logging.error(f"Unexpected error in extract_list_ar_invoices: {e}")
        return []
