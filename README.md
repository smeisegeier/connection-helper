# connection-helper

![PyPI - Version](https://img.shields.io/pypi/v/connection-helper) ![GitHub last commit](https://img.shields.io/github/last-commit/smeisegeier/connection-helper?logo=github) ![GitHub License](https://img.shields.io/github/license/smeisegeier/connection-helper?logo=github) ![py](https://img.shields.io/badge/python-3.10_|_3.11_|_3.12-blue.svg?logo=data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAxMDAgMTAwIj4KICA8ZGVmcz4KICAgIDxsaW5lYXJHcmFkaWVudCBpZD0icHlZZWxsb3ciIGdyYWRpZW50VHJhbnNmb3JtPSJyb3RhdGUoNDUpIj4KICAgICAgPHN0b3Agc3RvcC1jb2xvcj0iI2ZlNSIgb2Zmc2V0PSIwLjYiLz4KICAgICAgPHN0b3Agc3RvcC1jb2xvcj0iI2RhMSIgb2Zmc2V0PSIxIi8+CiAgICA8L2xpbmVhckdyYWRpZW50PgogICAgPGxpbmVhckdyYWRpZW50IGlkPSJweUJsdWUiIGdyYWRpZW50VHJhbnNmb3JtPSJyb3RhdGUoNDUpIj4KICAgICAgPHN0b3Agc3RvcC1jb2xvcj0iIzY5ZiIgb2Zmc2V0PSIwLjQiLz4KICAgICAgPHN0b3Agc3RvcC1jb2xvcj0iIzQ2OCIgb2Zmc2V0PSIxIi8+CiAgICA8L2xpbmVhckdyYWRpZW50PgogIDwvZGVmcz4KCiAgPHBhdGggZD0iTTI3LDE2YzAtNyw5LTEzLDI0LTEzYzE1LDAsMjMsNiwyMywxM2wwLDIyYzAsNy01LDEyLTExLDEybC0yNCwwYy04LDAtMTQsNi0xNCwxNWwwLDEwbC05LDBjLTgsMC0xMy05LTEzLTI0YzAtMTQsNS0yMywxMy0yM2wzNSwwbDAtM2wtMjQsMGwwLTlsMCwweiBNODgsNTB2MSIgZmlsbD0idXJsKCNweUJsdWUpIi8+CiAgPHBhdGggZD0iTTc0LDg3YzAsNy04LDEzLTIzLDEzYy0xNSwwLTI0LTYtMjQtMTNsMC0yMmMwLTcsNi0xMiwxMi0xMmwyNCwwYzgsMCwxNC03LDE0LTE1bDAtMTBsOSwwYzcsMCwxMyw5LDEzLDIzYzAsMTUtNiwyNC0xMywyNGwtMzUsMGwwLDNsMjMsMGwwLDlsMCwweiBNMTQwLDUwdjEiIGZpbGw9InVybCgjcHlZZWxsb3cpIi8+CgogIDxjaXJjbGUgcj0iNCIgY3g9IjY0IiBjeT0iODgiIGZpbGw9IiNGRkYiLz4KICA8Y2lyY2xlIHI9IjQiIGN4PSIzNyIgY3k9IjE1IiBmaWxsPSIjRkZGIi8+Cjwvc3ZnPgo=)

## usage

install / update package

```bash
pip install connection-helper -U
```

include in python

```python
from connection_helper import sql, pgp, sec
```

## why use connection-helper

`connection-helper` bundles some packages for connecting to sql databases

- `sql` is added as convenience wrapper for retrieving data from sql databases
  - `connect_sql()` to get get data from `['mssql', 'sqlite','postgres']`
  - `load_sql_to_sqlite` connect to a sql db and transfer a list of tables to `sqlite`
  - `load_sqlite_to_parquet()` to get all tables from a sqlite file as parquets
  - `unpack_files_to_duckdb()` return a tuple of all files of a dir (csv or parquet) into high performance duckdb objects ⚡
  - `print_meta()` print meta information of a certain database (niche case)
  - 🆕 `load_from_mssql()` load data from an MSSQL database into a Pandas DataFrame
  - 🆕 `save_to_mssql()` save data from a Pandas DataFrame to an MSSQL database
  - [ ] azure storage connector 🚧

- `pgp` cryptographic tools adapted from [python-gnupg](https://github.com/vsajip/python-gnupg). this is a wrapper around `gnupg`, but only offers some cenvenience or tailored options. feel free to use the original library or the [GNU Privacy Guard](https://gnupg.org/).
  - `encrypt()` a message for one or more recipient(s) with a public key
  - `decrypt()` a message with a private key. Passphrase must be provided via env variable
  - `find_key()` in keyring
  - ...

> 💡 The pgp methods require a pgp public/private key in the keyring

- `sec` secrets module. ⚠️ this package is optional and must be extra installed with `pip install 'connection-helper[sec]'` (enclose in '')
  - 🆕 `get_infisical_secrets()` retrieve secrets from [Infisical](https://infisical.com/) project (requires existing account and machine identities)
