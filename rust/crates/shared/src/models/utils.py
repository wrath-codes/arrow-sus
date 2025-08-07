# Type aliases
PathLike = Union[str, pathlib.Path]
FileContent = Dict[str, Union["Directory", "File"]]
T = TypeVar("T")

# Constants
CACHEPATH: Final[str] = os.getenv(
    "PYSUS_CACHEPATH", os.path.join(str(pathlib.Path.home()), "pysus")
)
__cachepath__: Final[pathlib.Path] = pathlib.Path(CACHEPATH)
__cachepath__.mkdir(exist_ok=True)


def to_list(item: Union[T, List[T], Tuple[T, ...], None]) -> List[T]:
    """Parse any builtin data type into a list"""
    if item is None:
        return []
    return [item] if not isinstance(item, (list, tuple)) else list(item)


# Cache storage
DIRECTORY_CACHE: Dict[str, "Directory"] = {}
