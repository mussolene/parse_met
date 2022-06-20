import argparse
import json
import os


def config_cli():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-cfg",
        "--config",
        type=str,
        default=os.getcwd() + "/config.json",
        help="Путь к файлу конфигурации",
    )
    args = parser.parse_args()

    config = json.load(open(args.config, "r", encoding="utf-8"))
    return config
