#!/usr/bin/env python3

import requests
import json
from os import getenv as env

class IMISClientConfig:
	def __init__(self, base_url, username, pwd):
		self.BASE_URL = base_url
		self.USERNAME = username
		self.PWD = pwd

class IMISClient:
    def __init__(self, config):
        # Validate configuration attributes
        if not config.BASE_URL:
            raise ValueError("Base URL is required in the configuration (e.g. https://demo123.imiscloud.com)")
        if not config.USERNAME:
            raise ValueError("Username is required in the configuration.")
        if not config.PWD:
            raise ValueError("Password is required in the configuration.")
        
        self.base_url = config.BASE_URL
        self.token = self.authenticate(config.USERNAME, config.PWD)

    def authenticate(self, username, password):
        if not username or not password:
            raise ValueError("Username and password are required for authentication.")
        token = self.request_token(username, password)
        if not token:
            raise Exception("Failed to authenticate with the API")
        return token

    def request_token(self, username, password):
        data = {"grant_type": "password", "username": username, "password": password}
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        token_url = f"{self.base_url}/token"
        response = self.make_request("POST", token_url, data, headers)
        return f"Bearer {response.json().get('access_token')}"

    def make_request(self, method, url, data=None, headers=None, params=None):
        try:
            response = requests.request(method, url, data=data, headers=headers, params=params)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            self.log_error(f"Request failed: {e}")
            return None

    def fetch_iqa(self, iqa_path, limit=None, page_size=200, last_updated=None):
        """
        Fetches data from a specified IQA path.

        This method retrieves data from an IMIS IQA endpoint. The data retrieval is paginated and can be
        limited to a certain number of records.

        Parameters:
        iqa_path (str): The IQA API path to fetch data from. It should be in the format:
                        "$/Path/To/Query"
                        Example: "$/Samples/Events/Event Attendees"
        limit (int, optional): The maximum number of records to fetch. If None, fetches all records.
        page_size (int): The number of records to fetch in each request (pagination).
        last_updated (str, optional): A date string to fetch records updated after this date.
        	add this as a filter in the IQA query and make it optional with value of "constant".

        Returns:
        list: A list of dictionaries, where each dictionary represents a record fetched from the IQA path.
        """
        items = []
        has_next = True
        offset = 0
        
		#construct the path request for IQAs in iMIS
        iqa_path = "/api/IQA?QueryName=" + iqa_path
        # print(iqa_path)
        while has_next and (limit is None or offset < limit):
            path_request = self.construct_path_request(iqa_path, page_size, offset, last_updated)
            response = self.make_request("GET", path_request, headers={"Authorization": self.token})
            if response:
                result = self.process_response(response)
                if result:
                    items.extend(result["items"])
                    has_next = result["has_next"]
                    offset += len(result["items"])
                    self.log_info(f"Retrieving: {offset} of {result['count']}")
                else:
                    break
            else:
                break

        return items

    def construct_path_request(self, path, page_size, offset, last_updated):
        path_request = f"{self.base_url}{path}&limit={page_size}&offset={offset}"
        if last_updated:
            path_request += f"&parameter={last_updated}"
        return path_request

    def process_response(self, response):
        try:
            data = response.json()
            return {
                "items": self.simplify_data(data),
                "has_next": data.get("HasNext", False),
                "count": data.get("TotalCount", 0)
            }
        except (json.JSONDecodeError, KeyError) as e:
            self.log_error(f"Error processing response: {e}")
            return None

    def simplify_data(self, data):
        def flatten_record(row):
            value_pairs = row["Properties"]["$values"]
            rec = {}
            for value in value_pairs:
                # Handle cases where 'Value' is a dictionary with a '$value' key
                if isinstance(value.get("Value"), dict):
                    rec[value["Name"]] = value["Value"].get("$value", value["Value"])
                else:
                    rec[value["Name"]] = value.get("Value")
            return rec

        return [
            flatten_record(item) for item in data.get("Items", {}).get("$values", [])
        ]


    def log_error(self, message):
        print(f"⛑️ Error: {message}")

    def log_info(self, message):
        print(f"ℹ️ Info: {message}")

# Additional methods for person data operations would go here

# Usage example
# config = IMISClientConfig()
# api_client = IMISClient(config)
