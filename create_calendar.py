from fpdf import FPDF

class CalendarPDF(FPDF):
    def __init__(self):
        super().__init__()
        # Добавляем Helvetica с поддержкой кириллицы
        self.add_font('Helvetica', '', '/System/Library/Fonts/Helvetica.ttc')

# Создание PDF
pdf = CalendarPDF()
pdf.add_page()
pdf.set_font('Helvetica', '', 28)
pdf.cell(0, 20, 'Январь 2026', 0, 1, 'C')

# Параметры сетки
col_width = 30
row_height = 50
margin_left = 10
margin_top = 50

# День недели
days_of_week = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']
pdf.set_xy(margin_left, margin_top - 10)
pdf.set_font('Helvetica', '', 10)

for day in days_of_week:
    pdf.cell(col_width, 8, day, 0, 0, 'C')
pdf.ln()

# Начало первого дня недели (Среда = 3)
first_day_of_week = 2  # Среда
january_days = 31

# Отслеживание позиции
current_col = first_day_of_week
current_row = 0
day_counter = 1

# Рисуем дни
pdf.set_font('Helvetica', '', 10)

while day_counter <= january_days:
    x = margin_left + current_col * col_width
    y = margin_top + current_row * row_height
    
    # Проверка границ страницы
    if y + row_height > 270:
        pdf.add_page()
        pdf.set_font('Helvetica', '', 28)
        pdf.cell(0, 20, 'Январь 2026 (продолжение)', 0, 1, 'C')
        pdf.set_xy(margin_left, 50)
        pdf.set_font('Helvetica', '', 10)
        for day in days_of_week:
            pdf.cell(col_width, 8, day, 0, 0, 'C')
        pdf.ln()
        y = 58
        current_row = 0
        current_col = 0
    
    pdf.set_xy(x, y)
    
    # Выходные выделяем
    if current_col >= 5:  # Сб и Вс
        pdf.set_fill_color(255, 230, 230)
        pdf.cell(col_width, row_height, '', 1, 0, 'L', True)
    else:
        pdf.cell(col_width, row_height, '', 1, 0, 'L')
    
    # Номер дня в верхнем левом углу
    pdf.set_xy(x + 2, y + 2)
    pdf.set_font('Helvetica', '', 14)
    pdf.cell(col_width - 4, 6, str(day_counter), 0, 0)
    
    # Переход на следующую позицию
    day_counter += 1
    current_col += 1
    
    if current_col >= 7:
        current_col = 0
        current_row += 1

pdf.output('/Users/yuriy/Desktop/Проекты/Стилва/Календарь_Январь_2026.pdf')
print('✅ PDF создан: /Users/yuriy/Desktop/Проекты/Стилва/Календарь_Январь_2026.pdf')
