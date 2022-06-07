import sys

from PyPDF2 import PdfReader

reader = PdfReader(sys.argv[1])

# reading all the pages content one by one
with open(sys.argv[2], "w", encoding="utf-8") as output:
    for page in reader.pages:
        output.write(page.extract_text())
        output.write("\n")
