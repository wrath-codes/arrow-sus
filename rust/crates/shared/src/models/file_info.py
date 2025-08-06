class FileInfo(TypedDict):
    """File information dictionary type"""

    size: Union[int, str]
    type: str
    modify: datetime
