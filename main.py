import csv
import datetime
import os

from core.cli import config_cli
from core.dataexp import ExcelWriter, compare_pricelist
from core.parser import get_pricelist


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


def write_csv(city, path_csv, holdings):
    data = get_pricelist(city, holdings)
    # Очищаем файл перед записью

    file_name = get_file_name(city, path_csv)

    open(file_name, "w", encoding="windows-1251").close()

    with open(file_name, "w", encoding="windows-1251") as ouf:
        writer = csv.writer(ouf, delimiter=";", lineterminator="\n")
        writer.writerows(data)
    return file_name


def main():
    config = config_cli()
    debug = int(config["DEFAULT"]["debug"])
    path_csv = config["DEFAULT"]["work_dir"]
    city_list = config["DEFAULT"]["city"].split(",")
    holdings = config["DEFAULT"]["holdings"].split(",")

    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    if not debug:
        for city in city_list:
            write_csv(city=city, path_csv=path_csv, holdings=holdings)

    files = {}
    files_dir = os.listdir(path_csv)
    for i in files_dir:
        if i.startswith("met23_"):
            key = i.split("_")[1]
            if files.get(key) is None:
                files[key] = []
            files[key].append(path_csv + "/" + i)

    data = compare_pricelist(files=files, config=config)

    with ExcelWriter(path_csv + "/data.xlsx", engine="xlsxwriter") as writer:
        for city, s in data:
            if not s.empty:
                sheet_name = f"{city}_met100"
                s.to_excel(writer, sheet_name=sheet_name)

    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


if __name__ == "__main__":

    main()
