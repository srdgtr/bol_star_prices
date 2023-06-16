import asyncio
import configparser
import json
import os
from datetime import datetime
from pathlib import Path

import aiohttp
import dropbox
import numpy as np
import pandas as pd
import requests

date_now = datetime.now().strftime("%c").replace(":", "-")
general_settings = configparser.ConfigParser()
general_settings.read(Path.home() / "general_settings.ini")
dbx = dropbox.Dropbox(general_settings.get("dropbox", "api_dropbox"))


def token_login(client_id, client_secret):
    token = json.loads(requests.post(general_settings.get("bol_api_urls","authorize_url"), auth=(client_id, client_secret)).text)[
        "access_token"
    ]
    if token:  # add right headers
        post_header = {
            "Accept": "application/vnd.retailer.v10+json",
            "Content-Type": "application/vnd.retailer.v10+json",
            "Authorization": "Bearer " + token,
        }
        # logger.info(f"login met client {client_id}")
        return post_header


ean_from_basis_file = pd.read_csv(
    max(Path("../bol_ftp").glob("**/basis_sorted_PriceList_*.csv"), key=os.path.getctime),
    usecols=["Product ID eigen","ean"],
    converters={"ean": "{:0>13}".format},
).query("`Product ID eigen`.str.startswith('WIN')")

ean_from_basis_file_unique = ean_from_basis_file["ean"].drop_duplicates()

# welke winkel maakt niet uit
first_shop= list(general_settings["bol_winkels_api"].keys())[0]
client_id, client_secret,_,_ = [x.strip() for x in general_settings.get("bol_winkels_api", first_shop).split(",")]

invalid_ean_numbers_new = []


async def get_bol_star_prices(session, ean, post_header):
    try:
        async with session.get(general_settings.get("bol_api_urls","base_url") + "/insights/price-star-boundaries/" + ean, headers=post_header
        ) as response:
            if response.status == 200:
                return await response.json()
            else:
                invalid_reponse = await response.json()
                status = invalid_reponse["status"]
                detail = invalid_reponse["detail"]
                instance = invalid_reponse["instance"]
                invalid_ean_numbers_new.append((instance, status, detail))
    except (AssertionError, aiohttp.ClientPayloadError, aiohttp.ClientResponseError) as s:
        print(s)


async def process_bol_star_prices(eans, post_header):
    timeout = aiohttp.ClientTimeout(total=240)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        response_bol_info = []
        for ean in eans:
            price_star_prices = await get_bol_star_prices(session, ean, post_header)
            if price_star_prices:
                star_info = {
                    "ean": ean,
                    "date_stars": price_star_prices["lastModifiedDateTime"][:10]
                }
                for level in price_star_prices["priceStarBoundaryLevels"]:
                    star_info[f"star {level['level']}"] = level["boundaryPrice"]
                response_bol_info.append(star_info)
        return pd.DataFrame(response_bol_info)


def get_bol_allowable_prices(eans):
    post_header = token_login(client_id, client_secret)
    return asyncio.run(process_bol_star_prices(eans.tolist(), post_header))

ean_from_basis_file_unique_splited = np.array_split(ean_from_basis_file_unique, 40)

get_bol_allowable_price = []
for eans in ean_from_basis_file_unique_splited:
    get_bol_allowable_price.append(get_bol_allowable_prices(eans))

get_bol_allowable_price_totaal = pd.concat(get_bol_allowable_price, ignore_index=True)
get_bol_allowable_price_totaal.to_csv(f"bol_star_prices_{date_now}.csv", index=False)


all_invalid_ean_numbers = pd.DataFrame(invalid_ean_numbers_new, columns=["status","detail","instance"])
all_invalid_ean_numbers.to_csv(f"invalid_numbers_stars_{date_now}.csv", index=False)
