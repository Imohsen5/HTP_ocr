import hashlib  # For calculating file hash
import os

import pytesseract
from PIL import Image
from pymongo import MongoClient

# Configure the path to the Tesseract executable
pytesseract.pytesseract.tesseract_cmd = r"X:\Program Files\Tesseract-OCR\tesseract.exe"

# MongoDB connection
client = MongoClient("localhost", 27017)
db = client["document_db"]
collection = db["documents"]


def calculate_file_hash(file_path):
    """Calculate SHA-256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        while True:
            data = f.read(65536)  # Read in 64k chunks
            if not data:
                break
            sha256.update(data)
    return sha256.hexdigest()


def scan_and_store_document(doc_folder):
    """Scan document folder, extract text from each page using Tesseract, and store in MongoDB."""
    for filename in os.listdir(doc_folder):
        if filename.endswith(".jpg") or filename.endswith(".png"):
            image_path = os.path.join(doc_folder, filename)

            # Calculate file hash to check if page was already processed
            file_hash = calculate_file_hash(image_path)

            # Check if page already exists in database by hash
            if collection.find_one({"file_hash": file_hash}):
                print(f"Skipping {filename}: Page already processed.")
                continue

            # Process and store page
            try:
                with Image.open(image_path) as img:
                    text = pytesseract.image_to_string(img)
            except Exception as e:
                print(f"Error processing {image_path}: {e}")
                continue

            # Store in MongoDB
            document = {"text": text, "file_path": image_path, "file_hash": file_hash}
            collection.insert_one(document)
            print(f"Page stored in MongoDB for {filename}")


def process_documents(base_dir):
    """Process all document folders in the base directory."""
    for doc_folder in os.listdir(base_dir):
        full_doc_folder = os.path.join(base_dir, doc_folder)
        if os.path.isdir(full_doc_folder):
            scan_and_store_document(full_doc_folder)


def main():
    # Base directory containing document folders
    base_documents_dir = r"X:\htpcode\ocrproject\document_folders"

    # Process all document folders in the base directory
    process_documents(base_documents_dir)


if __name__ == "__main__":
    main()
