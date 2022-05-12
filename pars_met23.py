import argparse
import csv
import datetime
import os
import re
import subprocess
import sys

import numpy as np
import pandas as pa
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
        print(f"Страница {url} не найдена")
        return None
    if r.status_code == 429:
        print("Получено ошибку 429, пробуем пройти капчу")
        p = subprocess.Popen("chrome.exe", str(url))
        p.wait()
        return make_request(url, count + 1)
    return r


def get_pricelist(city, holdings, filter_name):
    d = []

    if city == "msk":
        root_url = "http://23met.ru/plist/"
    else:
        root_url = "http://" + city + ".23met.ru/plist/"

    for hold in holdings:
        url_hold = root_url + hold
        if city != "" and city != "msk":
            url_hold = root_url + hold + city

        r = make_request(url_hold)
        if r is None:
            continue
        # суки отсылают сломанный html
        text = r.text.replace("<tbody>", "<tbody><tr>")
        text = text.replace("</tr>", "</tr><tr>")

        soup = BeautifulSoup(text, "html.parser")
        table_prices = soup.find_all("table", {"class": "tablesorter"})

        for table in table_prices:
            rows_prices = table.tbody.find_all("tr")
            for row in rows_prices:
                if not row.contents:
                    continue

                product = row.contents[0].get_text()
                if filter_name and not any(
                    f.lower() in product.lower() for f in filter_name
                ):
                    continue

                length = row.contents[2].get_text()
                length = length.replace(" ", "х")
                steel = row.contents[4].get_text()
                dop = row.contents[6].get_text()
                gost = row.contents[8].get_text()

                qt1, price1 = get_qt_price(row.contents[9])
                qt2, price2 = get_qt_price(row.contents[11])

                d.append(
                    [product, length, dop, steel, gost, qt1, price1, qt2, price2, hold]
                )

    return d


def get_qt_price(row):
    s = row.find("small")
    qt = 0 if (s is None) else s.get_text().replace(" ", "")

    s = row.find("span")
    price = 0
    if s is not None:
        price = s.get_text().replace(" ", "")
        price = regex.sub(r"[^0-9,]", "", price)
        price = 0 if price == "" else float(price)

    return qt, price


def write_csv(city, path_csv, holdings, filter_name):
    data = get_pricelist(city, holdings, filter_name)
    # Очищаем файл перед записью

    if city == "":
        city_path = ""
    else:
        city_path = city + "_"

    file_name = (
        path_csv
        + "/met23_"
        + city_path
        + datetime.datetime.now().strftime("%Y-%m-%d %H.%M.%S")
        + ".csv"
    )
    open(file_name, "w", encoding="windows-1251").close()

    with open(file_name, "w", encoding="windows-1251") as ouf:
        writer = csv.writer(ouf, delimiter=";", lineterminator="\n")
        writer.writerows(data)
    return file_name


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--city",
        type=lambda s: re.split("[ ,;]", s),
        default=["ekb", "msk"],
        help="список городов ekb, moskva, spb",
    )
    parser.add_argument(
        "-p",
        "--path",
        type=str,
        default=os.getcwd(),
        help="Путь к файлу для сохранения",
    )
    parser.add_argument(
        "-hl",
        "--holdings",
        type=lambda s: re.split("[ ,;]", s),
        default=["agrupp", "mc", "ntpz", "ktzholding"],
        help="Список холдингов для парсинга agrupp, mc, ntpz, ktzholding",
    )
    parser.add_argument(
        "-fn",
        "--filter_name",
        type=lambda s: re.split("[ ,;]", s),
        default=[],
        help="Фильтр по названию можно задать списком через запятую Арматура, Труба и т.д.",
    )
    args = parser.parse_args()
    return args.city, args.path, args.holdings, args.filter_name


def compare_pricelist(files, path_csv, holdings):

    dataset = []
    names = [
        "product",
        "length",
        "dop",
        "steel",
        "gost",
        "qt1",
        "price1",
        "qt2",
        "price2",
        "hold",
    ]
    dtypes = {"price1": "float64", "price2": "float64"}

    for city, file in files:

        df = pa.read_csv(
            file,
            encoding="windows-1251",
            skipinitialspace=True,
            quotechar="'",
            sep=";",
            engine="python",
            header=None,
            names=names,
            dtype=dtypes,
        )

        df["price2"] = np.where(df["price2"], df["price2"], df["price1"])

        s = pa.pivot_table(
            df,
            index=["product", "steel", "length"],
            columns="hold",
            values="price2",
            aggfunc=np.max,
            fill_value=0,
        ).reindex(holdings, axis=1, fill_value=0)
        dataset.append((city, s))

    with pa.ExcelWriter(path_csv + "/data.xlsx", engine="xlsxwriter") as writer:
        for city, s in dataset:
            sheet_name = f"{city}_met100"
            s.to_excel(writer, sheet_name=sheet_name)
            workbook = writer.book
            fmt = workbook.add_format().set_align("left")

            writer.sheets[sheet_name].set_column(0, 0, 50)
            writer.sheets[sheet_name].set_column("B:F", 20, fmt)


if __name__ == "__main__":

    city_list, path_csv, holdings, filter = cli()

    files = []

    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    for city in city_list:
        files.append(
            (
                city,
                write_csv(
                    city=city, path_csv=path_csv, holdings=holdings, filter_name=filter
                ),
            )
        )

    # files.append(("ekb", path_csv + "/met23_ekb_2022-05-12 12.12.32.csv"))
    # files.append(("msk", path_csv + "/met23_msk_2022-05-12 12.13.52.csv"))

    compare_pricelist(files, path_csv, holdings)

    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
