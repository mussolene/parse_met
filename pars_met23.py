import argparse
import csv
import datetime
import os
import re
import subprocess
import sys

import numpy as np
import regex
import requests
from bs4 import BeautifulSoup
from pandas import ExcelWriter, merge, pivot_table, read_csv


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


def get_pricelist(city, holdings):
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

                product = row.contents[0].get_text().strip()
                length = (
                    row.contents[2]
                    .get_text()
                    .strip()
                    .replace("  ", "х")
                    .replace(".", ",")
                )
                link = (
                    row.find_all("span")[0]
                    .attrs["data-link"]
                    .replace(root_url.replace("plist/", ""), "")
                    .split("/")[1]
                )
                sizes = re.findall("[0-9.]+", length)
                steel = row.contents[4].get_text()

                price1 = get_qt_price(row.contents[9])
                price2 = get_qt_price(row.contents[11])

                if not filter_row(link, sizes):
                    continue

                d.append(
                    [
                        now,
                        product,
                        length,
                        "Ст3" if not steel.strip() else steel.strip(),
                        price2 if price2 else price1,
                        hold,
                    ]
                )

    return d


def filter_row(link, sizes) -> bool:

    res_size = filter_size_prod(link, sizes)
    if not res_size:
        return False

    return True


def filter_size_prod(link, sizes):
    res = False
    if (
        not (link == "tryba_es_kvadr" or link == "tryba_es_pr" or link == "tryba_es")
        or len(sizes) < 2
    ):
        return False

    if len(sizes) == 2:
        first_size = int(sizes[0])
        third_size = 0
    else:
        first_size = int(sizes[0])
        third_size = int(sizes[2])

    if link == "tryba_es":
        if 57 <= first_size <= 325:
            res = True
    else:
        if 40 <= first_size <= 200 and 2 <= third_size <= 10:
            res = True

    return res


def get_qt_price(row):
    s = row.find("span")
    price = 0
    if s is not None:
        price = s.get_text().replace(" ", "")
        price = regex.sub(r"[^0-9,]", "", price)
        price = 0 if price == "" else float(price)

    return price


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


def write_csv(city, path_csv, holdings, filter_name):
    data = get_pricelist(city, holdings)
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
        default=["ekb", "msk", "spb"],
        help="список городов ekb, msk, spb",
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

    args = parser.parse_args()
    return args.city, args.path, args.holdings


def _pivot_table(df):

    s = pivot_table(
        df,
        index=["product", "length", "steel"],
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
            df = read_csv(
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

        frame = city_data[0]
        for i in range(1, len(city_data)):

            frame = frame.append(city_data[i])

            df_diff_price = merge(
                city_data[i],
                city_data[i - 1],
                on=["product", "length", "steel", "hold"],
                how="outer",
            )

            df_diff_price = df_diff_price.fillna("0")
            df_diff_price["date"] = (
                df_diff_price["date_x"].max() + "_" + df_diff_price["date_y"].max()
            )
            df_diff_price["price"] = [
                float(x.price_x if x.price_x != "0" else 0)
                - float(x.price_y if x.price_y != "0" else 0)
                for x in df_diff_price.itertuples()
            ]

            frame = frame.append(df_diff_price)

        dataset.append((value, _pivot_table(frame.reindex(columns=names).fillna(0))))

    with ExcelWriter(path_csv + "/data.xlsx", engine="xlsxwriter") as writer:
        for city, s in dataset:
            sheet_name = f"{city}_met100"
            s.to_excel(writer, sheet_name=sheet_name)


def main(debug=False):

    city_list, path_csv, holdings = cli()

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

    main()
