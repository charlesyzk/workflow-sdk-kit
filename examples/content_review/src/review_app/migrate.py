"""数据库建表入口；生产中应使用独立迁移账号执行。"""

from .container import get_storage


def main() -> None:
    get_storage().create_tables()
    print("content review workflow tables are ready")


if __name__ == "__main__":
    main()
