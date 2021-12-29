import argparse
from pydantic import BaseModel

from python_starter.python_starter import *



def main():
	set_pandas_options()
	parser: argparse.ArgumentParser = argparse.ArgumentParser(
		description="Parsing Configuration files"
	)
	parser.add_argument(
		"input_dir",
		type=str,
		help="argument",
	)
	parser.add_argument(
		"output_dir",
		type=str,
		help="Directory to output updated Warp themes.",
	)
	args = parser.parse_args()
	output_folder: str = args.output_dir
	generate_sub_paths_for_folder(output_folder)

	f = open()
	parse_file()

	input_dir: str = args.input_dir

	
	filename: str = import_single_file(
		folder=platform,
		list_filename_filter_conditions=(platform,),
	)
	model = platform_modeler.parse_file(os.path.join(platform, filename))
	data = model.data()
	del model
	dataframe: pd.DataFrame = pd.DataFrame(
		data,
	)
	print(dataframe)
	output_filename = NT_output_filename(
		output_filename=platform,
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
	mopen()
