# parse_met
Для сборки приложения в exe нужно запустить auto-py-to-exe

```pyinstaller --noconfirm --onefile --console --add-data "C:/GIT/me/public/parser_met/core;core/" --additional-hooks-dir "C:/GIT/me/public/parser_met/core" --hidden-import "numpy" --hidden-import "pandas" --hidden-import "beautifulsoup4" --hidden-import "regex" --hidden-import "argparse" --hidden-import "configparser" --hidden-import "requests"  "C:/GIT/me/public/parser_met/main.py"```