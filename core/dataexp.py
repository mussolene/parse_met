import numpy as np
from pandas import ExcelWriter, merge, pivot_table, read_csv


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


def compare_pricelist(files, config):
    list_zavod = config["DEFAULT"]["list"].split(",")
    list_link = config["DEFAULT"]["link"].split(",")

    dataset = []
    names = [
        "date",
        "product",
        "length",
        "link",
        "size_1",
        "size_2",
        "size_3",
        "steel",
        "price",
        "hold",
    ]

    dtypes = {"price": "float64"}

    for city, value in enumerate(files):
        city_data = []
        files_city = files[value]
        for file in files_city:
            df = get_dataframe(names, dtypes, file, list_link)
            city_data.append(df)
        for i in list_zavod:
            key_list = value + "_" + i
            frame = get_filled_frame(city_data, config[key_list])
            dataset.append(
                (key_list, _pivot_table(frame.reindex(columns=names).fillna(0)))
            )

    return dataset


def get_dataframe(names, dtypes, file, list_link):
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
    df = df[df["link"].isin(list_link)]
    return df


def get_filled_frame(city_data, filters):
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

    frame = frame[frame["size_1"].isin(filters["width"].split(","))]
    frame = frame[frame["size_3"].isin(filters["thick"].split(","))]

    return frame
