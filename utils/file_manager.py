
import pathlib

class FileManager:

    def __init__(self, base_directory):
        self.base_dir = base_directory

    def get_directory_name(self, date: str):
        """Returns the table name for given date"""
        year, month = date.split("-")[0], date.split("-")[1]
        return f"{self.base_dir}/{year}/{month.zfill(2)}"

    def create_directory(self, date: str):
        """Creates a directory for the given date"""
        dir_name = pathlib.Path(self.get_directory_name(date))
        if not dir_name.exists():
            dir_name.mkdir(parents=True, exist_ok=True)
            print(f"[+] Created directory {dir_name}")
        else:
            print(f"[+] Directory {dir_name} already exists")

    def make_directory_structure(self, start_year, end_year):
        """Creates a directory structure for the given year range"""
        for year in range(start_year, end_year):
            for month in range(1, 13):
                date = f"{year}-{str(month).zfill(2)}-01"
                self.create_directory(date)