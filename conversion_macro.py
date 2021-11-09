import pandas as pd
import numpy as np
import argparse
import warnings
import pathlib
import time
import csv
import sys
import ast
import re


"""
This script should be used to migrate from the raw results
file, generated using the ConvertXMLtoFinalCSV.exe script, 
to a more user-friendly format that can be opened and manipulated
easily within Excel. 

Conversion: CSV -> CSV
"""

__author__ = "Kevin Ludwig"
__date__ = "03/18/2020"

""" Ignore annoying warnings. """
warnings.simplefilter("ignore")

""" Enable large CSV files to be read. """
maxInt = sys.maxsize
sizeError = True
while sizeError:
    sizeError = False
    try:
        csv.field_size_limit(maxInt)
    except OverflowError:
        maxInt = int(maxInt / 5)
        sizeError = True


def _print_line(line: str, stage_num: int, space_char: str) -> None:
    """
    Prints the lines indicating what stage 
    of the process is being completed.
    """

    string = "{:<%s}{:%s>%s}" % (0, space_char, 70 - len(line))
    print(string.format(line, "stage " + str(stage_num) + "/9"))


def read_results(doc_path: pathlib.Path) -> list:
    """
    Read in the results document using a CSV
    reader object.
    rtype: list
    """

    _print_line("Reading results file", 1, "")
    if not doc_path.is_file() or doc_path.suffix != ".csv":
        raise ValueError(
            "\r\nERROR: This is not a valid document. Please check your input and try again."
        )

    try:
        file = open(doc_path, "r", encoding="utf-8")
        data = [row for row in csv.reader(file, delimiter=",")]
    except UnicodeDecodeError:
        file = open(doc_path, "r", encoding="latin-1")
        data = [row for row in csv.reader(file, delimiter=",")]
    file.close()

    return data


def remove_scoring_section(data: list) -> list:

    """
    This removes the scoring section from
    the results CSV, which is not used
    in later stages of the analysis.
    """

    _print_line("Removing scoring section", 2, "")
    try:
        data[0]
    except Exception:
        raise ValueError(
            "\r\nERROR: This document might not be from the correct stage in the results process. "
            "\r\nConfirm this is from the stage after using the ConvertXMLtoFinalCSV.exe script."
        )

    first_section_found = False
    total_lines = len(data) - 1
    line_num = 0
    while not first_section_found and line_num <= total_lines:
        if len(data[line_num]) == 0:
            first_section_found = True
        line_num += 1

    return data[line_num:]


def create_dict_with_event_type(data: list) -> dict:

    """
    This breaks the results into chunks,
    each chunk representing the results
    from a specific event type. The 
    event type is stored as a dictionary
    key, with the value being the results
    chunk corresponding to that event
    type.
    """

    _print_line("Splitting results by event type", 3, "")
    dict_data = dict()
    event_type = None
    for line in data:
        if len(line) == 1 and "-----" not in line[0]:
            event_type = line[0]
            dict_data[event_type] = list()
        elif len(line) > 1:
            dict_data[event_type].append(line)

    return dict_data


def create_df_for_each_event_type(dict_data: dict) -> dict:

    """
    Migrates the results chunks for each
    event type into dataframes. Again, the
    dictionary key is the event type and the
    value is the dataframe. Each dataframe is
    then given a new column, which contains
    the event type as its value. The column
    is located in the second position (after
    'Main Company').
    """

    return {
        et: pd.DataFrame(
            columns=data[0] + ["Event Type"], data=[x + [et] for x in data[1:]]
        ).drop(columns=[""])
        for et, data in dict_data.items()
    }


def _rename_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:

    """
    [INTERNAL] Used to rename duplicate columns 
    using the convention 'C_N', where 'C' is 
    the column name.
    """

    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        cols[cols[cols == dup].index.values.tolist()] = [
            "{}_{}".format(dup, str(idx)) if idx != 0 else dup
            for idx in range(sum(cols == dup))
        ]
    df.columns = cols
    return df


def merge_dataframes(data_dict: dict) -> pd.DataFrame:

    """
    Merges all of the individual event
    type dataframes into one large dataframe.
    Contains language to avoid issues that
    arise when individual event types have 
    duplicate slot columns.
    """

    _print_line("Merging event types into single dataframe", 4, "")
    new_data = pd.DataFrame()
    for et, data in data_dict.items():
        data.rename(columns={data.columns[1]: "Article Date"}, inplace=True)
        data = _rename_duplicate_columns(data)
        new_data = new_data.append(data, sort=False, ignore_index=True)
    new_data.replace(r"^\s*$", np.nan, regex=True, inplace=True)

    for column in new_data.filter(regex=r"_[0-9]+$").columns:
        base_et = re.findall(r"^([^_]*)_[0-9]+$", column)[0]
        new_data[base_et].fillna(new_data[column], inplace=True)

        new_data.loc[new_data[column] == new_data[base_et], column] = np.nan

    new_data = new_data.rename(columns=lambda x: re.sub(r"_[0-9]+$", "", x))
    return new_data


def add_neutral_polarity(data: pd.DataFrame) -> pd.DataFrame:

    """
    Adds the string 'NEUTRAL' to the polarity
    column for all extractions of neutral
    polarity; currently, those cells are
    simply left blank.
    """

    _print_line("Cleaning data", 5, "")
    try:
        data["Polarity"]
    except KeyError:
        return data
    except Exception:
        raise ValueError("\r\nERROR: Results look empty...")

    data["Polarity"] = data["Polarity"].replace(np.nan, "NEUTRAL")
    data["Polarity"] = data["Polarity"].str.replace(r"^$", r"NEUTRAL", regex=True)
    return data


def remove_dashes_from_sentence_start(data: pd.DataFrame) -> pd.DataFrame:

    """
    Sentences that start with at least one
    '-' produce an error when viewed in 
    Excel, where the value appears as "#NAME".
    This would remove those dashes (which
    are unnecessary to the structure of
    the data) in order to avoid this
    """

    try:
        data["Sentence"] = data["Sentence"].str.replace(
            r"^[ ]*(-)+[ ]*", "", regex=True
        )
    except KeyError:
        pass
    return data


def pull_other_entity_values(data: pd.DataFrame, option: bool):

    """
    Pulls out any missing slot values
    from the 'Other' column into their
    respective columns iff that cell
    was blank. Temporarily renames 
    identical column names with _X, where
    X = column index, then replaces the
    names back to their original form.
    """

    if not option:
        _print_line('SKIPPING "Other" column values', 6, ":")
        return data
    _print_line('Pulling extra slot values from "Other" column', 6, "")

    if "Other" not in data.columns.tolist():
        print("\r\nWARNING: There is no OTHER column in the data...")
        return data

    data["Other Len"] = data["Other"].str.len()
    data["Other"] = data["Other"].fillna("{}")
    data["Other Dict"] = ""
    for idx, row in data.iterrows():
        try:
            other_dict = ast.literal_eval(
                re.sub(r"(\{| )([^:,{ ]+?):", r'\1"\2":', row["Other"])
            )
        except SyntaxError:
            other_dict = dict()

        data.at[idx, "Other Dict"] = other_dict

    all_keys = [k for _, row in data.iterrows() for k, _ in row["Other Dict"].items()]

    for k in all_keys:
        if k not in data:
            data[k] = ""

    for idx, row in data.iterrows():
        for k, v in row["Other Dict"].items():
            data.loc[idx, k] = v if row[k] == "" else row[k]

    return data.drop(columns=["Other Dict"])


def delete_blank_columns(data: pd.DataFrame) -> pd.DataFrame:

    """
    Deletes slot columns that
    contain no slot values, as well
    as slot values that are repeats
    in the same extraction row.
    """

    _print_line("Deleting blank columns and duplicate slot values", 7, "")
    data.replace("", np.nan, inplace=True)
    data.dropna(axis="columns", how="all", inplace=True)
    return data


def reorder_columns(data: pd.DataFrame) -> pd.DataFrame:

    """
    Reorders columns into the 'normal' 
    order.
    """

    _print_line("Sorting the columns into their final order", 8, "")
    good_cols = [
        "Main Company",
        "Article Date",
        "Event Type",
        "Event Target",
        "Extraction",
        "Polarity",
        "Other",
        "Sentence",
        "URL",
        "Sentence(Inc. Annotations)",
    ]
    option_cols = ["Event Target", "Extraction", "Polarity"]

    for option in option_cols:
        if option not in data.columns.values:
            good_cols.remove(option)
    if "Other" not in data.columns.values:
        good_cols.remove("Other")

    split_num = 2 + len(option_cols)
    data = data[
        good_cols[:split_num]
        + [name for name in data if name not in good_cols]
        + good_cols[split_num:]
    ]
    return data


def write_document(data: pd.DataFrame, doc_path: pathlib.Path):

    """
    Writes the final document. It uses the
    same name as the original results, plus
    '_final' at the end.
    """

    _print_line("Writing document", 9, "")

    data.to_csv(doc_path, index=False, quoting=csv.QUOTE_ALL)


##############################################################################
##############################################################################
##############################################################################


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Converts the results CSV file into a more user-friendly CSV file."
    )
    parser.add_argument(
        "--document", required=True, help="The results document to convert (.csv)."
    )
    parser.add_argument(
        "--pullothervalues",
        required=False,
        action="store_true",
        help='Pull slot values from "Other" column to their respective columns (default FALSE).',
    )
    parser.add_argument(
        "--overwrite",
        required=False,
        action="store_true",
        help="Overwrites the converted file if it already exists",
    )
    args = parser.parse_args()

    start_time = time.perf_counter()
    document = pathlib.Path(args.document)
    final_document = document.parents[0] / f"{document.stem}_final.csv"
    if final_document.is_file() and not args.overwrite:
        raise FileExistsError(
            f"The final document ('{final_document}') has already been written! Use '--overwrite' to overwrite"
        )
    pull_others = args.pullothervalues

    all_data = read_results(document)  # read data from document
    all_data = remove_scoring_section(all_data)  # remove the top scoring data
    all_data = create_dict_with_event_type(
        all_data
    )  # break results into chunks corresponding to event type
    all_data = create_df_for_each_event_type(
        all_data
    )  # convert from array to pandas DataFrame for each event type
    all_data = merge_dataframes(all_data)  # merge all individual DataFrames into one
    all_data = add_neutral_polarity(all_data)  # adds 'NEUTRAL' to the 'Polarity' column
    all_data = remove_dashes_from_sentence_start(
        all_data
    )  # removes "-"s from the beginning of sentences
    all_data = pull_other_entity_values(
        all_data, pull_others
    )  # pull slot values from 'Other' column
    all_data = delete_blank_columns(
        all_data
    )  # delete slot columns containing no values
    all_data = reorder_columns(all_data)  # reorders columns
    write_document(all_data, final_document)  # write final document

    print(
        "\r\nAll done; finished in {} seconds.".format(
            str(round(time.perf_counter() - start_time, 2))
        )
    )
