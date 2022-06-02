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
    merged_steel_conf = config["DEFAULT"]["merge_steel"]

    merged_steel = []
    for steel_merge in merged_steel_conf.split(","):
        merged_steel.append(
            {"NAME": steel_merge, "VALUE": config[steel_merge]["list_steel"].split(",")}
        )

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

    dtypes = {
        "price": "float64",
        "size_1": "str",
        "size_2": "str",
        "size_3": "str",
        "steel": "str",
        "hold": "str",
        "link": "str",
        "product": "str",
        "length": "str",
        "date": "str",
    }

    for city, value in enumerate(files):
        city_data = []
        files_city = files[value]
        for file in files_city:
            df = get_dataframe(names, dtypes, file, list_link, merged_steel)
            city_data.append(df)
        for i in list_zavod:
            key_list = value + "_" + i
            if config.has_section(key_list):
                frame = get_filled_frame(city_data, config[key_list])
                dataset.append(
                    (key_list, _pivot_table(frame.reindex(columns=names).fillna(0)))
                )

    return dataset


def get_dataframe(names, dtypes, file, list_link, merged_steel):
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
    if merged_steel:
        for i in merged_steel:
            df.loc[(df["steel"].isin(i["VALUE"])), "steel"] = i["NAME"]

    df = df.groupby(
        ["date", "product", "length", "steel", "hold", "size_1", "size_2", "size_3"]
    )

    df = df.agg({"price": np.max})
    df = df.reset_index()
    df = df.sort_values(
        ["date", "product", "length", "steel", "hold", "size_1", "size_2", "size_3"]
    )

    return df


def get_filled_frame(city_data, filters):
    frame = city_data[0]
    for i in range(1, len(city_data)):
        frame = frame.append(city_data[i])

        df_diff_price = merge(
            city_data[i],
            city_data[i - 1],
            on=["product", "length", "steel", "hold", "size_1", "size_2", "size_3"],
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
        df_diff_price = df_diff_price.drop(["date_x", "date_y"], axis=1)
        df_diff_price = df_diff_price.drop(["price_x", "price_y"], axis=1)

        frame = frame.append(df_diff_price)

    for i in range(1, 3):
        size_filter = filters.get("size_" + str(i))
        if size_filter:
            frame = frame[frame["size_" + str(i)].isin(size_filter.split(","))]

    filter_name = filters.get("filter_name")
    if filter_name:
        filter_name_list = filter_name.split(",")
        for i in filter_name_list:
            frame = frame.loc[frame["product"].str.find(i) != -1]

    exclude_name = filters.get("exclude_name")
    if exclude_name:
        exclude_name_list = exclude_name.split(",")
        for i in exclude_name_list:
            frame = frame.loc[frame["product"].str.find(i) == -1]

    return frame
