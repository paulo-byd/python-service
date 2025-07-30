import hashlib
from pathlib import Path
import json

BASE_DIR = Path(__file__).parent
TEST_INPUT_PDF_DIRECTORY = BASE_DIR / "test_pdf_dir"

# Pseudo function to simulate PDF processing
def run_batch_processing(input_pdf_dir_path: Path, pdf_file_paths: list = []):
    """
    Processes a batch of PDF files.

    Args:
        input_pdf_dir_path (Path): The directory containing PDF files.
        pdf_file_paths (list): A list of specific PDF file paths to process. Defaults to an empty list.

    Returns:
        str: A JSON string where the key is a hashed 12-digit value of the file path prefixed to the file name,
             and the value is the file name.
    """
    if not pdf_file_paths:
        if not input_pdf_dir_path.exists():
            raise FileNotFoundError(f"The specified path '{input_pdf_dir_path}' does not exist.")
        # Get all PDFs in the directory and its subdirectories
        pdf_file_paths = list(input_pdf_dir_path.rglob("*.pdf"))

    result = {}
    for pdf_file in pdf_file_paths:
        # Generate a 12-digit hash of the file path
        file_path_hash = hashlib.sha256(str(pdf_file).encode()).hexdigest()[:16]
        # Create the key by prefixing the hash to the file name
        key = f"{file_path_hash}_<{pdf_file.name}>"
        # Add the key-value pair to the result
        result[key] = {"file_name" : pdf_file.name }

    # Return the result as a JSON string
    return json.dumps(result, indent=4)


if __name__ == "__main__":
    # Convert INPUT_PDF_DIRECTORY to string before passing it
    pdf_results_json = run_batch_processing(TEST_INPUT_PDF_DIRECTORY.resolve())
    # Print the JSON dictionary in a nicely formatted way
    print("\n--- JSON Results ---")
    print(json.dumps(json.loads(pdf_results_json), indent=4, sort_keys=True))