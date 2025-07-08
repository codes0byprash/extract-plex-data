import requests

def extract_list_ap_invoices(headers, url):
    transaction_date_begin = "2024-12-31T12:34:56.1234567Z"
    transaction_date_end = "2025-01-01T12:34:56.1234567Z"
    response = requests.get(url, headers=headers, params={
        'transactionDateBegin': transaction_date_begin,
        'transactionDateEnd': transaction_date_end
    })
    response.raise_for_status()
    return response.json()