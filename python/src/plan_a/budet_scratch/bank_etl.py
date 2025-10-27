import csv
import re
from pathlib import Path
from typing import List
import pdfplumber


def parse_td_bank_pdf_to_csv(
    pdf_path: str | Path, output_dir: str | Path = "./data/raw/td_bank"
) -> Path:
    """
    Parse a TD Bank PDF statement and extract transactions to a raw CSV file.

    Args:
        pdf_path: Path to the TD Bank PDF statement
        output_dir: Directory where the CSV output will be saved (default: ./data/raw/td_bank)

    Returns:
        Path to the generated CSV file
    """
    pdf_path = Path(pdf_path)
    output_dir = Path(output_dir)

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate output filename - use statement date from filename if possible
    output_filename = pdf_path.stem.replace(" ", "_") + "_transactions.csv"
    output_path = output_dir / output_filename

    transactions = []
    current_section = None

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            # Extract text from the page
            text = page.extract_text()

            if not text:
                continue

            lines = text.split("\n")

            for line in lines:
                line = line.strip()

                # Identify section headers
                if "ElectronicDeposits" in line or "ELECTRONICDEPOSITS" in line.upper():
                    current_section = "CREDIT"
                    continue
                elif (
                    "ElectronicPayments" in line or "ELECTRONICPAYMENTS" in line.upper()
                ):
                    current_section = "DEBIT"
                    continue
                elif line.startswith("Subtotal:") or "ACCOUNTSUMMARY" in line.upper():
                    current_section = None
                    continue

                # Skip header lines
                if "POSTINGDATE" in line.upper() or "DESCRIPTION" in line.upper():
                    continue

                # Parse transaction lines - format: MM/DD DESCRIPTION AMOUNT
                # Date pattern at start: MM/DD
                date_match = re.match(
                    r"^(\d{2}/\d{2})\s+(.+?)\s+([\d,]+\.\d{2})$", line
                )

                if date_match and current_section:
                    date = date_match.group(1)
                    description = date_match.group(2).strip()
                    amount = date_match.group(3).replace(",", "")

                    # Determine transaction type
                    trans_type = "CREDIT" if current_section == "CREDIT" else "DEBIT"

                    # Format for CSV matching the TD Bank format
                    debit = amount if trans_type == "DEBIT" else ""
                    credit = amount if trans_type == "CREDIT" else ""

                    transactions.append([date, trans_type, description, debit, credit])

    # Headers matching TD Bank CSV format (simplified)
    headers = ["Date", "Transaction Type", "Description", "Debit", "Credit"]

    # Write to CSV
    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        writer.writerows(transactions)

    print(f"Parsed {len(transactions)} transactions to {output_path}")
    return output_path


def parse_all_td_bank_pdfs(
    input_dir: str | Path = "./data/raw/td_bank",
    output_dir: str | Path = "./data/raw/td_bank",
) -> List[Path]:
    """
    Parse all TD Bank PDF statements in a directory.

    Args:
        input_dir: Directory containing TD Bank PDF statements
        output_dir: Directory where CSV outputs will be saved

    Returns:
        List of paths to generated CSV files
    """
    input_dir = Path(input_dir)
    output_paths = []

    # Find all PDF files in the input directory
    pdf_files = list(input_dir.glob("*.pdf"))

    if not pdf_files:
        print(f"No PDF files found in {input_dir}")
        return output_paths

    print(f"Found {len(pdf_files)} PDF file(s) to process")

    for pdf_file in pdf_files:
        try:
            output_path = parse_td_bank_pdf_to_csv(pdf_file, output_dir)
            output_paths.append(output_path)
        except Exception as e:
            print(f"Error processing {pdf_file.name}: {e}")

    return output_paths


if __name__ == "__main__":
    # Example usage: parse all PDFs in the default directory
    parse_all_td_bank_pdfs()
