
import importlib_metadata

from meal_datafeed import MealDatafeed as Datafeed


try:
    __version__ = importlib_metadata.version("vnpy_meal")
except importlib_metadata.PackageNotFoundError:
    __version__ = "dev"
