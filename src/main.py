from config import settings
import csv
import pydantic

from python_starter import *

DATA: str = "data"
OUTPUT: str = "output"


def main():
    set_pandas_options()

    input_folder: str = settings.input_directory
    output_folder: str = settings.output_directory
    generate_sub_paths_for_folder(output_folder)

    filename: str = import_single_file(
        folder=input_folder,
        list_filename_filter_conditions=(settings.filter_conditions,),
    )
    model = pydantic.BaseModel.parse_file(
        os.path.join(
            input_folder,
            filename,
        )
    )
    data = model.data()
    del model
    dataframe: pd.DataFrame = pd.DataFrame(
        data,
    )

    print(dataframe)
    output_filename = NT_output_filename(
        output_filename=settings.output_filename,
    )
    # noinspection PyTypeChecker
    dataframe.to_csv(
        path_or_buf=generate_filename(
            nt_filename=output_filename,
            extension=".csv",
            folder=output_folder,
        ),
        index=False,
        quoting=csv.QUOTE_ALL,
        escapechar="\\",
    )


if __name__ == "__main__":
    open()
