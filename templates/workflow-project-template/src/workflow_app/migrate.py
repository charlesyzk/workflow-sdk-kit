"""数据库初始化入口。

开发环境可直接运行该模块创建缺失表。生产 TiDB 建议由专用迁移账号执行，并让
API/Worker 使用权限更小的应用账号；所需索引可参考随模板交付的 SDK 文档。
"""

from .container import get_storage


def main() -> None:
    get_storage().create_tables()
    print("workflow tables are ready")


if __name__ == "__main__":
    main()
