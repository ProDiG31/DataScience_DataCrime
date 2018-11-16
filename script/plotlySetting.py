
import plotly
import os

print("[INFO] - Loading .env data ")
API_KEY = os.getenv("ploty_api_key")
USERNAME = os.getenv("ploty_username")

print("[INFO] - Credential Loaded")
# print (API_KEY)
# print (USERNAME)

print("[INFO] - plotly crendentials set")
plotly.tools.set_credentials_file(username=USERNAME, api_key=API_KEY)
