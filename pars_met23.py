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
    now = datetime.datetime.now().strftime("%Y-%m-%d")

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
                length = length.strip().replace("  ", "х").replace(".", ",")

                if not filter_row(row, root_url):
                    continue

                steel = row.contents[4].get_text()
                dop = row.contents[6].get_text()
                gost = row.contents[8].get_text()

                qt1, price1 = get_qt_price(row.contents[9])
                qt2, price2 = get_qt_price(row.contents[11])

                d.append(
                    [
                        now,
                        product,
                        length,
                        steel,
                        price2 if price2 else price1,
                        hold,
                    ]
                )

    return d


def filter_row(row, root_url) -> bool:

    res = False
    length = row.contents[2].get_text()
    length = length.strip().replace("  ", "х").replace(".", ",")

    link = (
        row.find_all("span")[0]
        .attrs["data-link"]
        .replace(root_url.replace("plist/", ""), "")
        .split("/")[1]
    )
    sizes = re.findall("[0-9.]+", length)

    if link == "tryba_es_kvadr" or link == "tryba_es_pr" or link == "tryba_es":

        if len(sizes) == 3:
            first_size = int(sizes[0])
            third_size = int(sizes[2])
        elif len(sizes) == 2:
            first_size = int(sizes[0])
            third_size = 0
        else:
            return False

        if link == "tryba_es":
            if first_size >= 57 and first_size <= 325:
                res = True
        else:
            if (
                first_size >= 40
                and first_size <= 200
                and third_size >= 2
                and third_size <= 10
            ):
                res = True

    return res


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


def get_file_name(city, path_csv):
    if city == "":
        city_path = ""
    else:
        city_path = city + "_"

    file_name = (
        path_csv
        + "/met23_"
        + city_path
        + datetime.datetime.now().strftime("%Y-%m-%d")
        + ".csv"
    )
    return file_name


def get_previous_file_name(city, path_csv):
    if city == "":
        city_path = ""
    else:
        city_path = city + "_"

    now = datetime.datetime.now()

    file_name = (
        path_csv
        + "/met23_"
        + city_path
        + (now - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        + ".csv"
    )
    return file_name


def write_csv(city, path_csv, holdings, filter_name):
    data = get_pricelist(city, holdings, filter_name)
    # Очищаем файл перед записью

    file_name = get_file_name(city, path_csv)

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


def pivot_table(df):

    s = pa.pivot_table(
        df,
        index=["product", "steel", "length"],
        columns=["date", "hold"],
        values="price",
        aggfunc=np.max,
        fill_value=0,
    )

    return s


def compare_pricelist(files, path_csv):

    dataset = []
    names = [
        "date",
        "product",
        "length",
        "steel",
        "price",
        "hold",
    ]

    dtypes = {"price": "float64"}

    for city, value in enumerate(files):
        city_data = []
        files_city = files[value]
        for file in files_city:
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
            city_data.append(df)
        frame = pa.concat(city_data)
        dataset.append((value, pivot_table(frame)))

    with pa.ExcelWriter(path_csv + "/data.xlsx", engine="xlsxwriter") as writer:
        for city, s in dataset:
            sheet_name = f"{city}_met100"
            s.to_excel(writer, sheet_name=sheet_name)


def main(debug=False):

    city_list, path_csv, holdings, filter = cli()

    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    if not debug:
        for city in city_list:
            write_csv(
                city=city, path_csv=path_csv, holdings=holdings, filter_name=filter
            )

    files = {}
    files_dir = os.listdir(path_csv)
    for i in files_dir:
        if i.startswith("met23_"):
            key = i.split("_")[1]
            if files.get(key) is None:
                files[key] = []
            files[key].append(path_csv + "/" + i)

    compare_pricelist(files, path_csv)

    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


if __name__ == "__main__":

    main(True)
