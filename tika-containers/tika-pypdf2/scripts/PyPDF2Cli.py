import PyPDF2
import sys
import io

with open(sys.argv[1], 'rb') as pdf_file:
    with open(sys.argv[2], 'w', encoding='utf-8') as output:
        pdf_reader = PyPDF2.PdfFileReader(pdf_file)
        # reading all the pages content one by one
        for page_num in range(pdf_reader.numPages):
            pdf_page = pdf_reader.getPage(page_num)
            output.write(pdf_page.extractText())
            output.write('\n')