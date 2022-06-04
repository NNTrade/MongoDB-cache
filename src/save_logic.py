from enum import Enum


class SaveLogic(Enum):
    ErrorOnDuplicateConfig = 1,
    ReplaceDuplicateConfig = 2,
    AddDuplicateConfig = 3
