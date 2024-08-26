from typing import Union, Optional, Tuple, List
import copy
import dataiku
import pandas as pd
import datetime
import io


class FileImporter:
    """
    This class is responsible for importing stacked files from a Sharepoint folder
    that match the file names passed as arguments. A subfolder can be specified
    in case you want to search in a specific subfolder.

    To import files, you can specify a single file name or a list of file names.
    Similarly, you can specify a single subfolder or a list of subfolders. For multiple
    files, ensure that the names list and the subfolders list are of the same length,
    or provide a single subfolder to search for all file names in that subfolder.
    Sheets can also be specified individually for each file or as a list of lists for multiple sheets.
    The class supports importing files in CSV, XLS, and XLSX formats. For other file types, it returns
    a binary stream of the file content.

    Args:
    folder (dataiku.Folder):
        The Sharepoint folder from which the file is to be imported.
    file_paths (List[str]):
        A list with all the file paths contained in the same folder.
    names (Union[str, List[str]]):
        The names of the files to be imported and stacked. If specifying a single file,
        provide a string. To import multiple files, provide a list of strings, where
        each string represents a file name or a substring to match in the file names.
    subfolders (Union[str, List[Union[str, int, None]], None]):
        The subfolder in which to search for the files. If specifying a single subfolder,
        provide a string. For multiple files, provide a list of strings, integers, or None values,
        where each entry corresponds to a subfolder path. None values can be used if no specific
        subfolder is required. If a list of names is provided but only a single subfolder, the same
        subfolder will be used for all names.
    sheets (Union[str, int, List[List[Union[str, int, None]]], None]):
        The list of sheets to import from each of the files. If specifying a single sheet,
        provide a string or integer. For multiple files, specify a list of lists, where each
        inner list contains the sheet names or indices for the corresponding file. For example,
        [['Sheet1', 'Sheet2'], 'Sheet3'] will import 'Sheet1' and 'Sheet2' from the first file,
        and 'Sheet3' from the second file.
    exact_match (bool):
        Indicates if the file names must exactly match the specified names. If True, only files
        with names that exactly match the specified names will be imported. If False, files with
        names containing the specified strings will be imported.
    sep (str):
        The separator to use when reading CSV files. Default is ";".
    headers (bool):
        Indicates if the files have headers. If True, the first row of the file will be used as
        column names. If False, the first row will be treated as data.
    binary_mode (bool):
        If True, unsupported file types will be stored as binary streams instead of being processed.
    concatenated (bool): If True, all imported files will be concatenated into a single DataFrame. If False,
        the individual DataFrames will be stored in a dictionary, even if they don't share all the columns.
    file_checker (bool):
        If True, raises a FileNotFoundError if no file matching the specified name is found. If False,
        returns an empty binary stream when no file is found.
    latest_match (bool):
        If True, imports the latest file. If False imports all the files that matches de name, depending
        on the value of exact_match.

    Attributes:
    df (dict or pd.DataFrame):
        A dictionary with the DataFrames of the imported files or a single DataFrame,
        depending on whether names is a single file or multiple files. If importing a single file,
        df will be a DataFrame. If importing multiple files, df will be a dictionary where each key
        is a file name and the value is the corresponding DataFrame. For unsupported file types,
        the values will be binary streams of the file content if binary_mode is True.
    df_concatenated (pd.DataFrame):
        The DataFrame with all the imported files concatenated. This DataFrame is created
        by concatenating all individual DataFrames. If importing a single file, df_concatenated
        will be the same as df. If importing multiple files, df_concatenated will contain all
        the data from the individual DataFrames stacked on top of each other.
    """

    def __init__(
        self,
        folder: dataiku.Folder,
        names: Union[str, List[str]],
        file_paths: Optional[List[str]] = None,
        subfolders: Optional[Union[str, List[Optional[Union[str, int]]]]] = None,
        sheets: Optional[Union[str, int, List[List[Optional[Union[str, int]]]]]] = 0,
        exact_match: bool = False,
        sep: str = ";",
        headers: bool = True,
        binary_mode: bool = False,
        concatenated: bool = False,
        file_checker: bool = False,
        latest_match: bool = True,
        skiprows: Optional[Union[int, List[int]]] = None,
        use_union: bool = False,
    ):
        self.folder = folder
        self.file_paths = (
            file_paths if file_paths else self.folder.list_paths_in_partition()
        )
        self.names = names if names else None
        self.subfolders = (
            [
                "/" + subfolder if not subfolder.startswith("/") else subfolder
                for subfolder in subfolders
            ]
            if isinstance(subfolders, list)
            else subfolders
        )
        self.sheets = sheets
        self.exact_match = exact_match
        self.sep = sep
        self.headers = headers
        self.binary_mode = binary_mode
        self.concatenated = concatenated
        self.file_checker = file_checker
        self.latest_match = latest_match
        self.skiprows = skiprows
        self.use_union = use_union
        self.ficheros_no_encontrados = (
            copy.deepcopy(names) if isinstance(names, list) else [names]
        )
        self.result = None
        self.result_concatenated = None

        self._fill_atributes()

    def _list_ordered_attributes(self) -> dict:
        """
        Returns the class attributes in an ordered dictionary.
        """
        return {
            "folder": self.folder,
            "file_paths": self.file_paths,
            "names": self.names,
            "subfolders": self.subfolders,
            "sheets": self.sheets,
            "df": self.result_concatenated,
            "exact_match": self.exact_match,
        }

    def _validate_inputs(self):
        """
        Validates the input parameters to ensure they meet the required conditions for importing files.

        This method checks the following:
        1. Ensures that file names are provided.
        2. If a single file name is provided, ensures that the subfolder is either a single string or None.
        3. If a list of file names is provided, ensures that subfolders are either a list of the same length,
        a single string, or None.
        4. If a list of sheets is provided, ensures that it has the same length as the list of file names.
        5. Verifies that all provided lists (names, subfolders, and sheets) have the same length, if applicable.

        Raises:
            ValueError: If any of the validation checks fail.
        """
        if not self.names:
            raise ValueError("No file specified for import")

        # Verificación para nombres individuales y subcarpetas
        if isinstance(self.names, str):
            if not isinstance(self.subfolders, (str, type(None))):
                raise ValueError(
                    "If a single file is specified, a single subfolder must also be specified."
                )
            if not isinstance(self.skiprows, (int, type(None))):
                raise ValueError(
                    "If a single file is specified, a single skiprows must also be specified."
                )
            if not isinstance(self.sheets, (str, int, type(None))) and not (
                isinstance(self.sheets, List) and len(self.sheets) == 1
            ):
                raise ValueError(
                    "If a single file is specified, a single sheet must be specified or a list with one element, that can be either a int, str or a sublist."
                )

        # Verificación para listas de nombres, subcarpetas y hojas
        if isinstance(self.names, List):
            if not isinstance(self.subfolders, (List, str, type(None))):
                raise ValueError(
                    "If a list of files is specified, subfolders must be either a list, a single subfolder, or None."
                )
            if isinstance(self.subfolders, List) and len(self.subfolders) != len(
                self.names
            ):
                raise ValueError(
                    "If a list of subfolders is provided, it must have the same length as the list of file names."
                )
            if isinstance(self.skiprows, List) and len(self.skiprows) != len(
                self.names
            ):
                raise ValueError(
                    "If a list of skiprows is provided, it must have the same length as the list of file names."
                )
            if not isinstance(self.sheets, (str, int, type(None))) and not (
                isinstance(self.sheets, List) and len(self.sheets) == len(self.names)
            ):
                raise ValueError(
                    "If a list of files is specified, sheets must be either a single sheet, None, or a list of sheets with the same length as the list of file names."
                )

    def _fill_atributes(self):
        """
        Validates the inputs, imports files individually, and concatenates them if required.

        This method first validates the input parameters to ensure they are correct.
        It then imports the files without concatenating them. If the concatenated attribute
        is set to True, it will concatenate all imported files into a single DataFrame and store it
        in self.result_concatenated.

        Raises:
            ValueError: If any of the input validations fail.
        """
        self._validate_inputs()
        self._import_files_without_concatenation()
        if self.concatenated:
            self._concatenate_files()

    def _import_files_without_concatenation(self):
        """
        Imports files individually and stores them in a dictionary or a single
        DataFrame in case of a single file.
        """
        self.result = {}

        def import_file(
            name,
            subfolder=None,
            sheet=None,
            result=None,
            skiprows=None,
        ):
            imported = FileImporter.import_file(
                folder=self.folder,
                file_paths=self.file_paths,
                file_name=name,
                subfolder=subfolder,
                sheet=sheet,
                exact_match=self.exact_match,
                sep=self.sep,
                headers=self.headers,
                binary_mode=self.binary_mode,
                file_checker=self.file_checker,
                latest_match=self.latest_match,
                result=result,
                skiprows=skiprows,
            )
            return imported

        if isinstance(self.names, str):
            imported = import_file(
                name=self.names,
                subfolder=self.subfolders,
                sheet=(
                    self.sheets[0] if isinstance(self.sheets, list) else self.sheets
                ),
                result=self.result,
                skiprows=self.skiprows,
            )
            if imported:
                self.ficheros_no_encontrados.remove(self.names)

        elif isinstance(self.names, list) and len(self.names) == 1:
            imported = import_file(
                name=self.names[0],
                subfolder=(
                    self.subfolders[0]
                    if isinstance(self.subfolders, list)
                    else self.subfolders
                ),
                skiprows=(
                    self.skiprows[0]
                    if isinstance(self.skiprows, list)
                    else self.skiprows
                ),
                sheet=(
                    self.sheets[0] if isinstance(self.sheets, list) else self.sheets
                ),
                result=self.result,
            )
            if imported:
                self.ficheros_no_encontrados.remove(self.names[0])

        elif isinstance(self.names, list):
            for i, name in enumerate(self.names):
                subfolder = (
                    self.subfolders
                    if isinstance(self.subfolders, str)
                    else (
                        self.subfolders[i]
                        if isinstance(self.subfolders, list)
                        else None
                    )
                )
                skiprows = (
                    self.skiprows[i]
                    if isinstance(self.skiprows, list)
                    else self.skiprows
                )
                sheet = (
                    self.sheets[i]
                    if (
                        isinstance(self.sheets, list)
                        and len(self.sheets) == len(self.names)
                    )
                    else self.sheets
                )

                imported = import_file(
                    name=name,
                    subfolder=subfolder,
                    skiprows=skiprows,
                    sheet=sheet,
                    result=self.result,
                )
                if imported:
                    self.ficheros_no_encontrados.remove(name)

        if len(self.result) == 1:
            self.result = list(self.result.values())[0]

    def _concatenate_files(self,) -> pd.DataFrame:
        """
        Imports and concatenates files into a single DataFrame.

        Args:
        use_union (bool): If True, use the union of all columns from all DataFrames.
                        If False, use the intersection of columns shared by all DataFrames.
                        Default is False.
        """
        if (
            (not isinstance(self.result, dict))
            or (not self.result)
            or (not all(isinstance(df, pd.DataFrame) for df in self.result.values()))
        ):
            return self.result

        # Obtener los conjuntos de columnas
        column_sets = [set(df.columns.tolist()) for df in self.result.values()]

        if not column_sets:
            return self.result
        elif self.use_union:
            # Encontrar todas las columnas únicas presentes en los DataFrames
            all_columns = sorted(set.union(*column_sets))
        else:
            # Encontrar las columnas comunes presentes en todos los DataFrames
            all_columns = sorted(set.intersection(*column_sets))

            # Añadir la columna 'Origin' a cada DataFrame
            for name, df in self.result.items():
                df["Origin"] = name
                # Reindexar para asegurarse de que todos los DataFrames tengan las columnas apropiadas
                self.result[name] = df.reindex(columns=all_columns + ["Origin"])

        # Concatenar los DataFrames que no están vacíos
        self.result_concatenated = pd.concat(
            [df for df in self.result.values() if not df.empty],
            ignore_index=True,
        )

    @staticmethod
    def downloader(
        folder: "dataiku.Folder",
        file_name: str,
        sheet: Optional[Union[str, int, List[Optional[Union[str, int]]]]] = 0,
        sep: str = ";",
        headers: bool = True,
        binary_mode: bool = False,
        skiprows: Optional[int] = None,
    ) -> Tuple[Union[pd.DataFrame, io.BytesIO], bool]:
        """
        Downloads a file from a Sharepoint folder and processes it based on the file type.

        Args:
            folder (dataiku.Folder): The Sharepoint folder from which the file is to be downloaded.
            file_name (str): The name of the file to be downloaded.
            sheet (Union[str, int, List[Optional[Union[str, int]]]], optional): The sheet(s) to be imported from the file. Default is 0.
            sep (str, optional): The separator to use when reading CSV files. Default is ";".
            headers (bool, optional): Indicates if the files have headers. Default is True.
            binary_mode (bool, optional): If True, unsupported file types will be stored as binary streams instead of being processed. Default is False.

        Returns:
            Tuple[Union[pd.DataFrame, io.BytesIO], bool]: A tuple containing the imported data (as a DataFrame or binary stream) and a boolean indicating success.
        """

        try:
            binary_file = FileImporter._read_file_from_sharepoint(folder, file_name)
            binary_result = io.BytesIO(binary_file)

            if binary_mode:
                return (binary_result, True)

            result = FileImporter._process_file(
                binary_result,
                file_name,
                sheet,
                sep,
                headers,
                skiprows,
            )
            print("Successfully imported the document from Sharepoint")
            return (result, True)

        except Exception as e:
            raise Exception(f"Could not import the document from Sharepoint: {e}")

    @staticmethod
    def _read_file_from_sharepoint(
        folder: "dataiku.Folder",
        file_name: str,
    ) -> bytes:
        """
        Reads a file from a Sharepoint folder and returns its binary content.

        Args:
            folder (dataiku.Folder): The Sharepoint folder from which the file is to be read.
            file_name (str): The name of the file to be read.

        Returns:
            bytes: The binary content of the file.

        Raises:
            Exception: If there is an error reading the file from Sharepoint.
        """
        try:
            with folder.get_download_stream(file_name) as stream:
                return stream.read()
        except Exception as e:
            raise Exception(f"Error reading file from Sharepoint: {e}")

    @staticmethod
    def _process_file(
        binary_result: io.BytesIO,
        file_name: str,
        sheet: Optional[Union[str, int, List[Optional[Union[str, int]]]]],
        sep: str,
        headers: bool,
        skiprows: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Processes a binary file stream and returns a DataFrame based on the file type.

        Args:
            binary_result (io.BytesIO): The binary stream of the file content.
            file_name (str): The name of the file.
            sheet (Union[str, int, List[Optional[Union[str, int]]]], optional): The sheet(s) to be imported from the file.
            sep (str): The separator to use when reading CSV files.
            headers (bool): Indicates if the files have headers.

        Returns:
            pd.DataFrame: The DataFrame containing the imported data.

        Raises:
            ValueError: If the file type is unsupported.
        """
        file_type = FileImporter.detect_file_type(binary_result.getvalue())

        if file_type == "csv":
            return FileImporter._read_csv(
                binary_result,
                sep,
                headers,
                skiprows,
            )
        elif file_type == "xls":
            return pd.read_excel(
                binary_result,
                sheet_name=sheet,
                engine="xlrd",
                header=0 if headers else None,
                skiprows=skiprows,
            )
        elif file_type == "xlsx":
            return pd.read_excel(
                binary_result,
                sheet_name=sheet,
                engine="openpyxl",
                header=0 if headers else None,
                skiprows=skiprows,
            )
        else:
            raise ValueError("Unsupported file type.")

    @staticmethod
    def _read_csv(
        binary_result: io.BytesIO,
        sep: str,
        headers: bool,
        skiprows: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Reads a CSV file from a binary stream and returns a DataFrame.

        Args:
            binary_result (io.BytesIO): The binary stream of the CSV file content.
            sep (str): The separator to use when reading the CSV file.
            headers (bool): Indicates if the files have headers.

        Returns:
            pd.DataFrame: The DataFrame containing the imported CSV data.

        Raises:
            ValueError: If the CSV file cannot be decoded with common encodings.
        """
        encodings = ["utf-8", "latin1", "iso-8859-1", "cp1252"]
        for encoding in encodings:
            try:
                return pd.read_csv(
                    binary_result,
                    encoding=encoding,
                    sep=sep,
                    header=0 if headers else None,
                    skiprows=skiprows,
                )
            except UnicodeDecodeError:
                binary_result.seek(0)  # Reiniciar el stream para el siguiente intento
        raise ValueError("Unable to decode the CSV file with common encodings.")

    @staticmethod
    def detect_file_type(binary_data: bytes) -> str:
        """
        Detects the file type based on the binary data.

        Args:
            binary_data (bytes): The binary data of the file.

        Returns:
            str: The detected file type ("csv", "xls", "xlsx", or "unknown").
        """
        if binary_data.startswith(b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"):
            return "xls"
        elif binary_data.startswith(b"\x50\x4B\x03\x04"):
            return "xlsx"
        elif binary_data.startswith(b"\xEF\xBB\xBF") or b"," in binary_data[:1024]:
            return "csv"
        else:
            return "unknown"

    @staticmethod
    def import_file(
        folder: "dataiku.Folder",
        file_paths: List[str],
        file_name: str,
        subfolder: Optional[str] = None,
        sheet: Optional[Union[str, int, List[Optional[Union[str, int]]]]] = 0,
        exact_match: bool = False,
        sep: str = ";",
        headers: bool = True,
        binary_mode: bool = False,
        file_checker: bool = True,
        latest_match: bool = True,
        result: Optional[dict] = None,
        skiprows: Optional[int] = None,
    ) -> bool:
        """
        Imports a file from a Sharepoint folder based on specified parameters.

        Args:
            folder (dataiku.Folder): The Sharepoint folder from which the file is to be imported.
            file_paths (List[str]): The list of file paths in the folder.
            file_name (str): The name of the file to be imported.
            subfolder (str, optional): The subfolder in which to search for the file. Default is None.
            sheet (Union[str, int, List[Optional[Union[str, int]]]], optional): The sheet(s) to be imported from the file. Default is 0.
            exact_match (bool, optional): Indicates if the file names must exactly match the specified names. Default is False.
            sep (str, optional): The separator to use when reading CSV files. Default is ";".
            headers (bool, optional): Indicates if the files have headers. Default is True.
            binary_mode (bool, optional): If True, unsupported file types will be stored as binary streams instead of being processed. Default is False.
            file_checker (bool, optional): If True, raises a FileNotFoundError if no file matching the specified name is found. Default is True.
            latest_match (bool, optional): If True, imports the latest file. If False, imports all files that match the name. Default is True.
            result (dict, optional): A dictionary to store the imported file content. Default is None.

        Returns:
            bool: True if the file is successfully imported, False otherwise.
        """
        if exact_match:
            if subfolder:
                files = [
                    file
                    for file in files
                    if file.startswith(subfolder) and (file_name == file)
                ]
            else:
                files = [file for file in file_paths if file_name == file]
        else:
            if subfolder:
                files = [
                    file
                    for file in file_paths
                    if file.startswith(subfolder) and (file_name in file)
                ]
            else:
                files = [file for file in file_paths if file_name in file]

        if (not files) and (file_checker):
            raise FileNotFoundError("No file found matching the specified name")

        elif (not files) and (not file_checker):
            return False

        if latest_match:
            latest_modification_date = datetime.datetime.min
            latest_file = None
            for file in files:
                info = folder.get_path_details(file)
                modification_date = datetime.datetime.fromtimestamp(
                    info["lastModified"] / 1000
                )

                if modification_date > latest_modification_date:
                    latest_modification_date = modification_date
                    latest_file = file

            if latest_file:  # Asegurarse de que latest_file no sea None
                result[file_name], imported = FileImporter.downloader(
                    folder,
                    latest_file,
                    sheet,
                    sep,
                    headers,
                    binary_mode,
                    skiprows=skiprows,
                )
                return imported
            else:
                return False

        else:
            counter = 0
            for file in files:
                name = file_name + f"_{counter}"
                result[name], imported = FileImporter.downloader(
                    folder,
                    file,
                    sheet,
                    sep,
                    headers,
                    binary_mode,
                    skiprows=skiprows,
                )
                counter += 1

            return imported
