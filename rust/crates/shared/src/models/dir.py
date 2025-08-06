class Directory:
    """
    Directory class with caching and lazy loading.

    The Directory class represents a directory in a file system and includes
    mechanisms for caching instances and lazy loading of directory content.
    When a Directory instance is created, it normalizes the provided path
    and caches the instance. The content of the directory is not loaded
    immediately; instead, it is loaded when the `content` property or the
    `load` method is accessed or called.

    Attributes:
        path (str): The normalized path of the directory.
        name (str): The name of the directory.
        parent (Directory): The parent directory instance.
        loaded (bool): Indicates whether the directory content has been loaded.
        __content__ (Dict[str, Union[File, Directory]]): A dictionary
            containing the directory's content, with names as keys and File or
            Directory instances as values.

    Methods:
        _normalize_path(path: str) -> str: Normalizes the given path.
        _get_root_directory() -> Directory: Returns the root directory
            instance, creating it if necessary.
        _init_root_child(name: str) -> None: Initializes a root child
            directory.
        _init_regular(parent_path: str, name: str) -> None: Initializes a
            regular directory.
        content() -> List[Union[Directory, File]]: Returns the content of the
            directory, loading it if necessary.
        load() -> Self: Loads the content of the directory and marks it as
            loaded.
    """

    name: str
    path: str
    parent: "Directory"
    loaded: bool
    __content__: Dict[str, Union[File, "Directory"]]

    def __new__(cls, path: str, _is_root_child: bool = False) -> "Directory":
        normalized_path = os.path.normpath(path)

        # Handle root directory case
        if normalized_path == "/":
            return cls._get_root_directory()

        # Return cached instance if exists
        if normalized_path in DIRECTORY_CACHE:
            return DIRECTORY_CACHE[normalized_path]

        # Use os.path.split for reliable path splitting
        parent_path, name = os.path.split(normalized_path)

        # Handle empty parent path
        if not parent_path:
            parent_path = "/"
        # Handle parent paths that don't start with /
        elif not parent_path.startswith("/"):
            parent_path = "/" + parent_path

        # Create new instance
        instance = super().__new__(cls)
        instance.path = normalized_path

        if _is_root_child:
            instance._init_root_child(name)
        else:
            instance._init_regular(parent_path, name)

        DIRECTORY_CACHE[normalized_path] = instance
        return instance

    @staticmethod
    def _normalize_path(path: str) -> str:
        """Normalizes the given path"""
        path = f"/{path}" if not path.startswith("/") else path
        return path.removesuffix("/")

    @classmethod
    def _get_root_directory(cls) -> Directory:
        """Returns the root directory instance, creating it if necessary"""
        if "/" not in DIRECTORY_CACHE:
            root = super().__new__(cls)
            root.parent = root
            root.name = "/"
            root.path = "/"
            root.loaded = False
            root.__content__ = {}
            DIRECTORY_CACHE["/"] = root
        return DIRECTORY_CACHE["/"]

    def _init_root_child(self, name: str) -> None:
        """Initializes a root child directory"""
        self.parent = DIRECTORY_CACHE["/"]
        self.name = name
        self.loaded = False
        self.__content__ = {}

    def _init_regular(self, parent_path: str, name: str) -> None:
        """Initializes a regular directory"""
        self.parent = Directory(parent_path)
        self.name = name
        self.loaded = False
        self.__content__ = {}

    @property
    def content(self) -> List[Union[Directory, File]]:
        """Returns the content of the directory, loading it if necessary"""
        if not self.loaded:
            self.load()
        return list(self.__content__.values())

    def load(self) -> Self:
        """Loads the content of the directory and marks it as loaded"""
        self.__content__ |= load_directory_content(self.path)
        self.loaded = True
        return self

    def reload(self):
        """
        Reloads the content of the Directory
        """
        self.loaded = False
        return self.load()

    def __str__(self) -> str:
        return self.path

    def __repr__(self) -> str:
        return self.path

    def __hash__(self):
        return hash(self.path)

    def __eq__(self, other):
        if isinstance(other, Directory):
            return self.path == other.path
        return False


def load_directory_content(path: str) -> FileContent:
    """Directory content loading"""
    content: FileContent = {}

    try:
        ftp = FTPSingleton.get_instance()
        ftp.cwd(path)
        path = path.removesuffix("/")

        def line_parser(line: str):
            if "<DIR>" in line:
                date, time, _, name = line.strip().split(maxsplit=3)
                modify = datetime.strptime(f"{date} {time}", "%m-%d-%y %I:%M%p")
                info = {"size": 0, "type": "dir", "modify": modify}
                xpath = f"{path}/{name}"
                content[name] = Directory(xpath)
            else:
                date, time, size, name = line.strip().split(maxsplit=3)
                modify = datetime.strptime(f"{date} {time}", "%m-%d-%y %I:%M%p")
                info: FileInfo = {
                    "size": size,
                    "type": "file",
                    "modify": modify,
                }
                content[name] = File(path, name, info)

        ftp.retrlines("LIST", line_parser)
    except Exception as exc:
        raise exc
    finally:
        FTPSingleton.close()

    to_remove = [
        name
        for name in content
        if name.upper().endswith(".DBF")
        and name.upper().replace(".DBF", ".DBC") in content
    ]

    for name in to_remove:
        del content[name]

    return content
