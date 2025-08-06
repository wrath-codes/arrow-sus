class File:
    """
    FTP File representation with improved type safety.

    This class provides methods for interacting with files on the DataSUS FTP
    server. It includes functionality for downloading files synchronously and
    asynchronously, as well as retrieving file information in a human-readable
    format.

    Attributes:
        name (str): The name of the file without the extension.
        extension (str): The file extension.
        basename (str): The full name of the file including the extension.
        path (str): The full path to the file on the FTP server.
        parent_path (str): The directory path where the file is located on the
            FTP server.
        __info (FileInfo): Metadata about the file, including size, type, and
            modification date.

    Methods:
        info() -> Dict[str, str]:
            Returns a dictionary with human-readable file information,
            including size, type, and modification date.

    """

    def __init__(self, path: str, name: str, info: FileInfo) -> None:
        self.name, self.extension = os.path.splitext(name)
        self.basename: str = f"{self.name}{self.extension}"
        self.path: str = (
            f"{path}/{self.basename}"
            if not path.endswith("/")
            else f"{path}{self.basename}"
        )
        self.parent_path: str = os.path.dirname(self.path)
        self.__info: FileInfo = info

    @property
    def info(self) -> Dict[str, str]:
        """Returns a dictionary with human-readable file information"""
        return {
            "size": humanize.naturalsize(self.__info["size"]),
            "type": f"{self.extension[1:].upper()} file",
            "modify": self.__info["modify"].strftime("%Y-%m-%d %I:%M%p"),
        }

    def __str__(self) -> str:
        return str(self.basename)

    def __repr__(self) -> str:
        return str(self.basename)

    def __hash__(self):
        return hash(self.path)

    def __eq__(self, other):
        if isinstance(other, File):
            return self.path == other.path
        return False
