import glob
import os
import shutil


def run_mapreduce_job(input_directory, output_directory, mapper, reducer):

    def read_records_from_input(input_directory):
        sequence = []
        path = os.path.join(input_directory, "*")
        files = glob.glob(path)
        for file in sorted(files):
            with open(file, "r", encoding="utf-8") as f:
                for line in f:
                    sequence.append((file, line))
        return sequence

    def save_results_to_output(result):
        filename = os.path.join(output_directory, "part-00000")
        with open(filename, "w", encoding="utf-8") as f:
            for key, value in result:
                f.write(f"{key}\t{value}\n")

    def create_success_file():
        filename = os.path.join(output_directory, "_SUCCESS")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("")

    def create_output_directory():
        if os.path.exists(output_directory):
            shutil.rmtree(output_directory)
        os.makedirs(output_directory)

    sequence = read_records_from_input(input_directory)
    pairs_sequence = mapper(sequence)
    pairs_sequence = sorted(pairs_sequence)
    result = reducer(pairs_sequence)
    create_output_directory()
    save_results_to_output(result)
    create_success_file()