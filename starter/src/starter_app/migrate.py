from .container import get_storage


def main():
    get_storage().create_tables()
    print("workflow SDK tables are up to date")


if __name__ == "__main__":
    main()
