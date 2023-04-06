import datetime
import re
import subprocess
import sys

import regex
import requests
from bs4 import BeautifulSoup


def make_request(url, count=0):
    if count > 10:
        return None
    try:
        r = requests.get(url)
    except Exception as e:
        sys.exit(f"Ошибка соединения {e}")

    if r.status_code == 404:
        print(f"{url} : failed")
        return None
    if r.status_code == 429:
        print("Получено ошибку 429, пробуем пройти капчу")
        p = subprocess.Popen("chrome.exe", str(url))
        p.wait()
        return make_request(url, count + 1)
    print(f"{url} : accepted")
    return r


def get_pricelist(city, holdings):
    d = []
    now = datetime.datetime.now().strftime("%Y-%m-%d")
    root_url = get_root_url(city)
    for hold in holdings:
        urls_hold = get_url_hold(city, root_url, hold)
        for url_hold in urls_hold:

            r = make_request(url_hold)
            if r is None:
                continue

            table_prices = BeautifulSoup(normilize_html(r), "html.parser").find_all(
                "table", {"class": "tablesorter"}
            )

            for table in table_prices:
                rows_prices = table.tbody.find_all("tr")
                fill_result(d, now, root_url, hold, rows_prices)

    return d


def fill_result(d, now, root_url, hold, rows_prices):
    for row in rows_prices:
        if not row.contents:
            continue

        d.append(get_value_from_html(root_url, row, hold, now))


def get_value_from_html(root_url, row, hold, now):
    product = row.contents[0].get_text().strip()
    length = row.contents[2].get_text().strip().replace("  ", "х").replace(".", ",")
    link = (
        row.find_all("span")[0]
        .attrs["data-link"]
        .replace(root_url.replace("plist/", ""), "")
        .split("/")[-2]
    )
    sizes = get_sizes_list(length)
    steel = row.contents[4].get_text()
    steel = "Ст3" if not steel.strip() else steel.strip()

    price1 = get_qt_price(row.contents[9])
    price2 = get_qt_price(row.contents[11])
    price = price2 if price2 else price1

    return [
        now,
        product,
        length,
        link,
        sizes[0],
        sizes[1],
        sizes[2],
        steel,
        price,
        hold,
    ]


def normilize_html(r):
    # суки отсылают сломанный html
    # text = r.text.replace("<tbody>", "<tbody><tr>")
    # text = text.replace("</tr>", "</tr><tr>")
    return r.text


def get_url_hold(city, root_url, hold):
    urls_hold = []
    url_hold = root_url + hold
    urls_hold.append(url_hold)
    if city != "" and city != "msk":
        url_hold = root_url + hold + city
        urls_hold.append(url_hold)

    return urls_hold


def get_root_url(city):
    if city == "msk":
        return "http://23met.ru/plist/"
    else:
        return "http://" + city + ".23met.ru/plist/"


def get_sizes_list(length):
    sizes = re.findall("[0-9.,]+", length)
    ret = ["0", "0", "0"]
    for i in range(len(sizes[:3])):
        ret[i] = sizes[i].replace(",", ".")
    return ret


def get_qt_price(row):
    s = row.find("span")
    price = 0
    if s is not None:
        price = s.get_text().replace(" ", "")
        price = regex.sub(r"[^0-9,]", "", price)
        price = 0 if price == "" else float(price)

    return price
