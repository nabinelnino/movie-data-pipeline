from database.models.movie_model import MovieModel as data_model
from database.syncdb import sync_main
from file_parser import FileParser
import pandas as pd
import csv
import asyncio
import time
import traceback
import shutil
import os


class ProcessClass:
    def __init__(self, config_dict) -> None:
        self.config_dict = config_dict
        self.download_folder = self.configure_from_dict(
            config_dict, "persistence_file_path")
        self.create_folder()
        self.batch_size = self.configure_from_dict(config_dict, 'batch_size')

    def configure_from_dict(self, config_dict: dict, config_key: str) -> None:
        """
        Function for configuring the connector from a given config dict.
        :param config_dict: A dict with the required config information.
        :param config_key: The key in the provided dict that contains the config
        """
        try:
            props = config_dict[config_key]
        except KeyError as exc:
            raise KeyError(
                f"A '{config_key}' dictionary of values must be included in config file"
            ) from exc
        return props

    def create_folder(self):
        # Check if the folder already exists
        path = self.download_folder
        if not os.path.exists(path):
            # Create the folder if it doesn't exist
            os.makedirs(path)
            print(f"Folder created at {path}")
        else:
            print(f"Folder already exists at {path}")

    async def convert_genre_to_csv(self):
        """
        Convert raw genre data to CSV format.
        Reads raw genre data from the specified file, processes it, and writes it to a CSV file.
        Raises:
        - Exception: Any unexpected error that occurs during the conversion process.
        """
        try:

            genre_info = self.configure_from_dict(
                self.config_dict, "genre_data")
            file_path = self.configure_from_dict(genre_info, "genre_json")
            self.genre_csv_file_name = self.configure_from_dict(
                genre_info, "genre_csv")
            header = self.configure_from_dict(genre_info, "header")

            data = FileParser(file_path).read_file()
            csvfile, writer = FileParser.write_csv(
                self.genre_csv_file_name, header)
            for genre, movie_info in data.items():
                if isinstance(movie_info, dict):
                    for movie_id, movie_name in movie_info.items():
                        writer.writerow(
                            {'genre': genre, 'id': movie_id, 'name': movie_name})
                else:
                    print(f"instance of {movie_info} must be of type dict")

        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            try:
                csvfile.close()
            except NameError:
                pass

    async def convert_year_to_csv(self):
        """
        Convert raw year data to CSV format.
        Reads raw year data from the specified file, processes it, and writes it to a CSV file.
        Raises:
        - Exception: Any unexpected error that occurs during the conversion process.
        """
        try:
            year_info = self.configure_from_dict(
                self.config_dict, "year_data")
            file_path = self.configure_from_dict(year_info, "year_json")
            self.year_csv_file_name = self.configure_from_dict(
                year_info, "year_csv")
            header = self.configure_from_dict(year_info, "header")

            data = FileParser(file_path).read_file()
            csvfile, writer = FileParser.write_csv(
                self.year_csv_file_name, header)
            if isinstance(data, dict):
                for year, info in data.items():
                    if year == '' or year is None or not info:
                        continue
                    movie_id_check = info.get('movie_ids', None)
                    if not movie_id_check:
                        print(f'error{year}')
                    if movie_id_check:
                        movie_ids = info['movie_ids'].split(',')
                        for movie_id in movie_ids:
                            writer.writerow(
                                {'year': year, 'frequency': info['freq'], 'id': movie_id})

                print(f"CSV file {self.year_csv_file_name} has been created.")

            else:
                print(f"{data} must be an instance of dictionary")

        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            try:
                csvfile.close()
            except NameError:
                pass

    async def delete_folder(self, folder_path: str):
        """
        Delete a folder and its contents.

        Parameters:
        - folder_path (str): The path to the folder to be deleted.

        Raises:
        - FileNotFoundError: If the specified folder does not exist.
        - PermissionError: If the user does not have permission to delete the folder.

        """
        try:
            folder_path = folder_path.lstrip("./")

            shutil.rmtree(folder_path)
            print(f"Folder '{folder_path}' successfully deleted.")
        except FileNotFoundError:
            print(f"Error: Folder '{folder_path}' not found.")
        except PermissionError:
            print(
                f"Error: Permission denied. Unable to delete '{folder_path}'.")

    async def combine_file(self):
        """
        Combine and process multiple CSV files into a single merged CSV file.

        Reads CSV files containing information about movies, genres, and years.
        Performs a series of data processing steps, including filtering NaN values,
        merging dataframes based on common 'id' columns, dropping specified columns,
        changing data types, renaming columns, and assigning a new primary key.

        Writes the final merged dataframe to a CSV file.

        Raises:
        - Exception: Any unexpected error that occurs during the file combination process.

        """
        try:
            movie_csv = self.configure_from_dict(
                self.config_dict, "movie_csv")
            combined_info = self.configure_from_dict(
                self.config_dict, "combine_file")
            info_df = FileParser.read_csv(movie_csv)
            info_df = FileParser.filter_nan_values(info_df)

            genre_df = FileParser.read_csv(self.genre_csv_file_name)
            genre_df = FileParser.filter_nan_values(genre_df)

            year_df = FileParser.read_csv(self.year_csv_file_name)
            year_df = FileParser.filter_nan_values(year_df)

            drop_columns = self.configure_from_dict(
                combined_info, "drop_columns")

            dtype_map = self.configure_from_dict(
                combined_info, 'dtype_map')

            merged_df = pd.merge(
                pd.merge(year_df, genre_df, on='id'), info_df, on='id')

            merged_df = merged_df.drop(columns=drop_columns).astype(
                dtype_map)

            table_pk = self.configure_from_dict(combined_info, 'table_pk')
            rename_cols = self.configure_from_dict(
                combined_info, 'rename_cols')

            merged_df.rename(columns=rename_cols, inplace=True)
            merged_df[table_pk] = range(1, len(merged_df)+1)

            merged_csv_file_location = self.configure_from_dict(
                combined_info, "merged_csv")

            FileParser.df_to_csv(merged_csv_file_location, merged_df, False)
            df = pd.read_csv(merged_csv_file_location)

        except Exception as e:
            print(f"An error occurred: {e}")


class DatabaseHandle:
    def __init__(self, config_dict) -> None:
        self.config_dict = config_dict

    async def loader(self, data_model):
        try:
            file_info = self.config_dict.get("combine_file", None)
            assert file_info is not None, "combine_file configuration is missing in config_dict"
            file_path = file_info.get("merged_csv", None)
            assert file_path is not None, "merged_csv path is missing in combine_file configuration"
            batch_size = self.config_dict.get("batch_size", None)
            batch = []
            with open(file_path, 'r') as file:
                data = csv.DictReader(file)

                for row in data:
                    batch.append(row)
                    if batch_size:
                        if len(batch) >= batch_size:
                            await sync_main(batch, data_model)
                            batch = []

                if len(batch) > 0:
                    await sync_main(batch, data_model)
        except Exception as e:
            if isinstance(e, RuntimeError):
                print("finished running")
            else:
                print("Found unexpected error")
                print("error type: ", type(e))
                print("error msg: ", str(e))
                print(
                    "-------------------------------Start Of Error Track Back-------------------------------"
                    "\n"
                )
                print(str(traceback.format_exc()))
                print(
                    "-------------------------------End Of Error Track Back---------------------------------"
                )

    async def run_loader(self, data_model):
        await self.loader(data_model)
    time.sleep(1)


async def main():
    CONFIG_FILE_PATH = "./configs/info_config.yaml"
    config_dict = FileParser.read_yaml(CONFIG_FILE_PATH)
    process = ProcessClass(config_dict)
    try:
        await asyncio.gather(process.convert_genre_to_csv(), process.convert_year_to_csv())
        await process.combine_file()
        db = DatabaseHandle(config_dict)
        await db.run_loader(data_model)
        delete_consume = config_dict.get("delete_consumed_files", None)
        folder_path = config_dict.get("persistence_file_path", None)
        if delete_consume:
            await process.delete_folder(folder_path)
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    asyncio.run(main())
