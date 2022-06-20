import csv
import datetime
import json
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
    path_xlsx = config["DEFAULT"]["data_dir"]

    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    if not debug:
        for city in city_list:
            write_csv(city=city, path_csv=path_csv, holdings=holdings)

    files = get_csv_files(path_csv)

    data = compare_pricelist(files=files, config=config)
    renames = {}
    for key in config["NAMES"].keys():
        renames[key] = config["NAMES"].get(key)

    date_data = datetime.datetime.now().strftime("%Y-%m-%d")

    with ExcelWriter(
        path_xlsx + f"/data_{date_data}.xlsx", engine="xlsxwriter"
    ) as writer:
        for city, s in data:

            s.index.rename(renames, inplace=True)
            s.columns.rename(renames, inplace=True)
            s.rename(renames, axis=1, level=0, inplace=True)
            s.rename(renames, axis=0, level=1, inplace=True)
            s.rename(renames, axis=1, level=1, inplace=True)
            s.rename(renames, axis=1, level=0, inplace=True)
            s.rename(renames, axis=0, level=0, inplace=True)

            if not s.empty:
                sheet_name = city
                for key, value in renames.items():
                    sheet_name = sheet_name.replace(key, value)
                s.to_excel(writer, sheet_name=sheet_name)

    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


def get_csv_files(path_csv):
    files = {}
    files_dir = os.listdir(path_csv)
    for i in files_dir:
        if i.startswith("met23_"):
            key = i.split("_")[1]
            if files.get(key) is None:
                files[key] = []
            files[key].append(path_csv + "/" + i)
    return files


def gen_config():
    config = config_cli()
    config_json = {}
    config_json["DEFAULT"] = {}
    for key in config["DEFAULT"].keys():
        config_json["DEFAULT"][key] = config["DEFAULT"][key]

    for section in config.sections():
        config_json[section] = {}
        for key in config[section]:
            config_json[section][key] = config[section][key]

    json.dump(
        config_json,
        open("config.json", "w", encoding="utf-8"),
        indent=4,
        ensure_ascii=False,
    )


if __name__ == "__main__":
    # gen_config()
    main()
