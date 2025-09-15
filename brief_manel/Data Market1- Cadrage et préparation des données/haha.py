import pandas as pd
import requests

def execute():
  requestUrl = "https://{environment}.intermarche.com/v1/v1/stores"
  requestHeaders = {
    "X-App-Name": "lolilol",
    "X-App-Version": "Production",
    "X-Api-Key": "frVpIEAy7AAXbVmaV8zq4cj8BvC3AmHLuLRqiytTgnjBvAYb",
    "Accept": "application/json"
  }

  response = requests.get(requestUrl, headers=requestHeaders)

  df = pd.DataFrame(response)

  return df


execute()
