import datetime
import itertools
import os
import re
from collections import namedtuple
from typing import Any
from typing import Callable
from typing import Dict
from typing import Generator
from typing import Iterable
from typing import Iterator
from typing import List
from typing import NamedTuple
from typing import Pattern
from typing import Tuple

# GLOBAL VARIABLES
ERROR_FOLDER: str = ""
ERROR_LIST: List[str] = []
ERROR_FILE_ORIGIN: str = ""
ERROR_TASK_ORIGIN: str = ""
PARTITION_GROUP: int = 1
PARTITION_TOTAL: int = 1

NT_error = namedtuple(
    "error",
    [
        "error",
    ],
)
NT_filename_errors = namedtuple(
    "NT_filename_errors",
    [
        "error_file_origin",
        "error_task_origin",
    ],
)
NT_output_filename = namedtuple(
    "NT_filename_output",
    [
        "output_filename",
        "extension",
    ],
)


def log_error(
    error: str,
    log: bool = False,
    bool_suppress_print: bool = False,
) -> None:
    global ERROR_LIST
    if error:
        ERROR_LIST.append(error)
    if not bool_suppress_print:
        if log:
            print(f"{log} : {error}")
        else:
            print(f"{error} : {error}")


def set_error_folder(
    error_folder: str,
) -> None:
    global ERROR_FOLDER
    ERROR_FOLDER = error_folder
    print(f"error_folder : {error_folder}")


def set_error_file_origin(
    file_origin: str,
) -> None:
    global ERROR_FILE_ORIGIN
    ERROR_FILE_ORIGIN = file_origin
    print(f"file_origin : {file_origin}")


def set_error_task_origin(
    task_origin: str,
) -> None:
    global ERROR_TASK_ORIGIN
    ERROR_TASK_ORIGIN = task_origin
    if task_origin:
        print(f"task_origin : {task_origin}")


def write_errors_to_disk(
    clear_task_origin: bool = True,
    folder_error: str = ERROR_FOLDER,
    bool_suppress_print: bool = False,
    overwrite: bool = True,
) -> None:
    global ERROR_LIST
    if not folder_error:
        folder_error = ERROR_FOLDER
    error_task_origin: str
    error_file_origin: str
    if ERROR_FILE_ORIGIN:
        error_file_origin = ERROR_FILE_ORIGIN
    else:
        error_file_origin = "unknown"
        print(f"No file origin was specified for the error log.")
    if ERROR_TASK_ORIGIN:
        error_task_origin = ERROR_TASK_ORIGIN
    else:
        error_task_origin = "unknown"
        print(f"No task origin was specified for the error log.")

    output_filename: str = generate_filename(
        nt_filename=NT_filename_errors(
            error_file_origin=error_file_origin,
            error_task_origin=error_task_origin,
        ),
        delimiter="-",
        folder=folder_error,
        extension=".txt",
    )
    output_write_type: str
    if overwrite:
        output_write_type = "w+"
    elif os.path.exists(output_filename):
        output_write_type = "a+"
    else:
        output_write_type = "w+"

    with open(output_filename, output_write_type) as error_log_file:
        current_datetime: str = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        error_log_file.write(f"{current_datetime} : {error_task_origin}")
        error_log_file.write("\n")

        error: str
        for error in ERROR_LIST:
            error_log_file.write(str(error))
            error_log_file.write("\n")
        error_log_file.write("\n")
        error_log_file.write("\n")

    ERROR_LIST = []
    if clear_task_origin:
        set_error_task_origin("")
    if not bool_suppress_print:
        print(f"Error log outputted to: {output_filename}")


def generate_filename(
    nt_filename: tuple,
    extension: str = "",
    delimiter: str = "",
    folder: str = "",
) -> str:
    filename_extension: str
    filename_without_extension: Tuple[str, ...]
    try:
        filename_extension = nt_filename.extension
        filename_without_extension = tuple(x for x in nt_filename if x != filename_extension)
    except AttributeError as err:
        log_error(
            error=str(err),
        )
        filename_extension = ""
        filename_without_extension: Tuple[str, ...] = nt_filename

    def check_extension(f_ext, ext) -> str:
        match f_ext, ext:
            case "", ext:
                return ext
            case f_ext, "":
                return f_ext
            case f_ext, ext:
                if f_ext == ext:
                    return f_ext
                else:
                    log_error(
                        error=f"Mismatch between filename_extension: {f_ext} and extension: {ext}"
                        )
                    return ""

    filename_extension = check_extension(filename_extension, extension)
    output: str = delimiter.join(filename_without_extension)
    if folder:
        output = folder + output
    if filename_extension:
        output = output + filename_extension
    return output


def parse_filename(
    filename: str,
    delimiter: str,
    named_tuple,  # NamedTuple annotation doesn't work when using it as a callable
    extension: str = "",
    dt_dict: Dict[str, Any] = None,
) -> tuple:
    if extension:
        filename = filename.replace(extension, "")
    split_filename: List[str] = filename.split(delimiter)
    parsed_filename: NamedTuple
    try:
        parsed_filename = named_tuple(*split_filename)
    except TypeError:
        error: str = "parse_filename-" "named_tuple_size_mismatch-" "filename"
        parsed_filename = NT_error(error)
        log_error(error=error)
    if dt_dict:
        # noinspection PyProtectedMember
        parsed_filename_dict: Dict[str, Any] = parsed_filename._asdict()
        list_casted_data: List[Any] = []
        for key, value in parsed_filename_dict.items():
            cast_type: Any = dt_dict.get(key, None)
            if cast_type:
                casted_value: Any
                if callable(cast_type):
                    casted_value = cast_type(value)
                else:
                    casted_value = cast(
                        cast_object=value,
                        cast_type=cast_type,
                    )
                list_casted_data.append(casted_value)
            else:
                list_casted_data.append(value)
        return named_tuple(*list_casted_data)
    else:
        return parsed_filename


def cast(
    cast_object: Any,
    cast_type: str,
) -> Any:
    if cast_type == "str":
        return str(cast_object)
    elif cast_type == "int":
        return int(cast_object)
    elif cast_type == "float":
        return float(cast_object)
    elif cast_type == "bool":
        return bool(cast_object)
    else:
        log_error(
            error=f"Cast Type: {cast_type} not found. Returning original argument: {cast_object}"
        )
        return cast_object


def flatten_list(
    list_of_lists: List[List[Any]],
) -> List[Any]:
    item: Any
    sublist: List[Any]
    flat_list: List[Any] = [item for sublist in list_of_lists for item in sublist]

    return flat_list


def filter_list_strings(
    list_strings: Iterable[str],
    list_string_filter_conditions: Tuple[str, ...] = (),
) -> Iterable[str]:
    if len(list_string_filter_conditions) > 0:
        list_strings = filter(
            lambda x: all(
                str(condition) in x for condition in list_string_filter_conditions
            ),
            list_strings,
        )

    return list_strings


def is_single_item(
    list_items: List[Any],
) -> bool:
    match len(list_items):
        case 0:
            log_error(error="filter_single_item-list_empty")
            return False
        case 1:
            return True
        case _:
            log_error(error="filter_single_item-list_length_greather_than_1")
            return False


def generate_sub_paths_for_folder(
    folder: str,
) -> None:
    directories: List[str] = folder.split("/")
    recursive_sub_directories: Iterator[str] = itertools.accumulate(
        directories, lambda x, y: "/".join([x, y])
    )
    sub_directory: str
    for sub_directory in recursive_sub_directories:
        if not os.path.isdir(sub_directory):
            os.mkdir(sub_directory)


def import_paths_from_folder(
    folder: str,
    list_paths_filter_conditions: Tuple[str, ...] = (),
    check_paths: bool = False,
    include_files: bool = True,
    include_folders: bool = False,
    ignore_hidden: bool = True,
) -> Generator[str, None, List[str]]:
    if os.path.exists(folder):
        list_function_checks_all_true: List[Callable] = []
        list_function_checks_any_true: List[Callable] = []

        if check_paths:
            if ignore_hidden:
                pattern_to_ignore: Pattern[str] = re.compile(r"^\.[A-Za-z]")
                is_not_hidden_file_function: Callable[[str], bool] = lambda x: not bool(
                    re.match(pattern_to_ignore, x)
                )
                list_function_checks_all_true.append(is_not_hidden_file_function)
            if include_files:
                is_file_function: Callable[[str], bool] = lambda x: os.path.isfile(
                    f"{folder}{x}"
                )
                list_function_checks_any_true.append(is_file_function)
            if include_folders:
                is_folder_function: Callable[[str], bool] = lambda x: os.path.isdir(
                    f"{folder}{x}"
                )
                list_function_checks_any_true.append(is_folder_function)
        if len(list_paths_filter_conditions) > 0:
            filter_all_conditions_function: Callable[[str], bool] = lambda x: all(
                str(condition).lower() in str(x).lower()
                for condition in list_paths_filter_conditions
            )
            list_function_checks_all_true.append(filter_all_conditions_function)

        path: str
        for path in os.listdir(folder):
            bool_all_true_check: bool = True
            if len(list_function_checks_all_true) > 0:
                bool_all_true_check = all(
                    [
                        function_check(path)
                        for function_check in list_function_checks_all_true
                    ]
                )
            bool_any_true_check: bool = True
            if len(list_function_checks_any_true) > 0:
                bool_any_true_check = any(
                    [
                        function_check(path)
                        for function_check in list_function_checks_any_true
                    ]
                )
            if bool_all_true_check and bool_any_true_check:
                yield path

    else:
        generate_sub_paths_for_folder(
            folder=folder,
        )
        return []


def import_single_file(
    folder: str,
    list_filename_filter_conditions: Tuple[str, ...],
) -> str:
    """

    :rtype: object
    """
    filename: str
    list_rest: List[str]
    list_filenames: List[str] = list(
        import_paths_from_folder(
            folder=folder,
            list_paths_filter_conditions=list_filename_filter_conditions,
        )
    )
    single_file: bool = is_single_item(list_filenames)
    if not single_file:
        log_error(
            error=(f"parse_filename-" f"{'_'.join(list_filename_filter_conditions)}"),
        )
        return ""
    else:
        first_file: str
        rest_of_files: List[str]
        first_file, *rest_of_files = list_filenames
        return first_file


if __name__ == "__main__":
    print("Not meant to be run as a standalone script. Exiting.")
    exit()
