import numpy as np
from pandas import ExcelWriter, concat, merge, pivot_table, read_csv


def _pivot_table(df):

    df_pivot = pivot_table(
        df,
        index=["geometry", "length", "steel", "size_1", "size_2", "size_3"],
        columns=["date", "hold"],
        values="price",
        aggfunc=np.max,
        fill_value=0,
    )

    df_pivot.sort_values(
        ["geometry", "size_1", "size_2", "size_3"], ascending=True, inplace=True
    )
    df_pivot = df_pivot.droplevel("geometry", axis=0)
    df_pivot = df_pivot.droplevel("size_1", axis=0)
    df_pivot = df_pivot.droplevel("size_2", axis=0)
    df_pivot = df_pivot.droplevel("size_3", axis=0)

    return df_pivot


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
        "geometry",
    ]

    dtypes = {
        "price": "float64",
        "size_1": "float64",
        "size_2": "float64",
        "size_3": "float64",
        "steel": "str",
        "hold": "str",
        "link": "str",
        "product": "str",
        "length": "str",
        "date": "str",
    }
    city_keys = files.keys()
    for city in city_keys:
        city_data = [
            get_dataframe(names, dtypes, file, list_link, merged_steel)
            for file in files[city]
        ]
        for i in list_zavod:
            key_list = city + "_" + i
            if config.get(key_list):
                frame = get_filled_frame(city_data, config[key_list])
                dataset.append(
                    (
                        key_list,
                        _pivot_table(
                            frame.reindex(
                                columns=names,
                            ).fillna(0),
                        ),
                    )
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

    df = df.loc[df["link"].isin(list_link)]
    merged_steel_list = []
    if merged_steel:
        for i in merged_steel:
            df.loc[(df["steel"].isin(i["VALUE"])), "steel"] = i["NAME"]
            merged_steel_list.append(i["NAME"])

    df = df.loc[df["steel"].isin(merged_steel_list)]

    df = df.groupby(
        ["date", "product", "length", "steel", "hold", "size_1", "size_2", "size_3"]
    )

    df = df.agg({"price": np.min})
    df = df.reset_index()
    size_3 = np.where(df["size_3"] == 0, df["size_2"], df["size_3"])
    size_2 = np.where(df["size_3"] == 0, 0, df["size_2"])

    df["size_3"] = size_3
    df["size_2"] = size_2

    df["geometry"] = np.where(df["size_1"] == df["size_2"], 0, 99)
    df["geometry"] = np.where(df["size_2"] == 0, 2, df["geometry"])
    df["geometry"] = np.where(df["geometry"] == 99, 1, df["geometry"])

    return df


def get_filled_frame(city_data, filters):
    dataframes = []
    dataframes.append(filter_frame(city_data[0], filters))
    for i in range(1, len(city_data)):
        frame1 = filter_frame(city_data[i], filters)
        frame2 = filter_frame(city_data[i - 1], filters)

        df_diff_price = merge(
            frame1,
            frame2,
            on=[
                "product",
                "length",
                "steel",
                "hold",
                "size_1",
                "size_2",
                "size_3",
                "geometry",
            ],
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

        dataframes.append(df_diff_price)
        dataframes.append(frame2)
        dataframes.append(frame1)

    frame = concat(dataframes)

    frame.drop_duplicates(inplace=True)
    frame.reset_index(inplace=True)
    frame.reindex(columns=frame.columns)

    return frame


def filter_frame(frame, filters):

    filled_frame = filter_size(frame, filters)

    filter_name = filters.get("filter_name")
    if filter_name:
        filter_name_list = filter_name.split(",")
        for i in filter_name_list:
            filled_frame = filled_frame.loc[filled_frame["product"].str.find(i) != -1]

    exclude_name = filters.get("exclude_name")
    if exclude_name:
        exclude_name_list = exclude_name.split(",")
        for i in exclude_name_list:
            filled_frame = filled_frame.loc[filled_frame["product"].str.find(i) == -1]

    return filled_frame


def filter_size(frame, filters):
    filled_frame = []

    filters_size = filters.get("filters")
    for fs in filters_size:
        frame_size = frame.copy()
        for i in range(1, 4):
            size_filter = fs.get("size_" + str(i))
            if size_filter:
                float_size = [0 if not x else float(x) for x in size_filter.split(",")]
                frame_size = frame_size.loc[
                    frame_size["size_" + str(i)].isin(float_size)
                ]
        filled_frame.append(frame_size)

    filled_frame = concat(filled_frame)

    filled_frame.drop_duplicates(inplace=True)
    return filled_frame
