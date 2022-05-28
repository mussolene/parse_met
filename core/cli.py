import argparse
import configparser
import os


def config_cli():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-cfg",
        "--config",
        type=str,
        default=os.getcwd() + "/config.ini",
        help="Путь к файлу конфигурации",
    )
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.config)
    return config
