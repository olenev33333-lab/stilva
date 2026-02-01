from xhtml2pdf import pisa

html_file = '/Users/yuriy/Desktop/Проекты/Стилва/calendar_print.html'
pdf_file = '/Users/yuriy/Desktop/Проекты/Стилва/Календарь_Январь_2026.pdf'

with open(html_file, 'rb') as f:
    pisa.CreatePDF(f, open(pdf_file, 'wb'))

print('✅ PDF создан успешно!')
