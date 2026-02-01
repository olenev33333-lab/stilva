from weasyprint import HTML, CSS
from io import BytesIO

html_content = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <title>Календарь Январь 2026</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 20px;
            background: white;
        }
        h1 {
            text-align: center;
            margin-bottom: 30px;
        }
        .calendar {
            width: 100%;
            border-collapse: collapse;
        }
        .calendar td {
            border: 2px solid #333;
            height: 100px;
            width: 14.28%;
            padding: 8px;
            vertical-align: top;
            position: relative;
        }
        .calendar .day-number {
            font-weight: bold;
            font-size: 16px;
            margin-bottom: 5px;
        }
        .calendar .day-name {
            font-size: 10px;
            color: #666;
            margin-bottom: 5px;
        }
        .calendar .weekend {
            background-color: #FFE5E5;
        }
        .calendar .notes {
            border-top: 1px dashed #ccc;
            margin-top: 5px;
            padding-top: 5px;
            font-size: 11px;
            color: #999;
            min-height: 50px;
        }
        .day-header {
            background-color: #f0f0f0;
            font-weight: bold;
            padding: 10px;
            text-align: center;
        }
        @page {
            size: A4;
            margin: 15mm;
        }
        .page-break {
            page-break-after: always;
        }
    </style>
</head>
<body>
    <h1>Январь 2026</h1>
    
    <table class="calendar">
        <tr>
            <td class="day-header">Пн</td>
            <td class="day-header">Вт</td>
            <td class="day-header">Ср</td>
            <td class="day-header">Чт</td>
            <td class="day-header">Пт</td>
            <td class="day-header">Сб</td>
            <td class="day-header">Вс</td>
        </tr>
        <tr>
            <td colspan="2"></td>
            <td><div class="day-number">1</div><div class="day-name">Ср</div><div class="notes">Напишите здесь</div></td>
            <td><div class="day-number">2</div><div class="day-name">Чт</div><div class="notes">Напишите здесь</div></td>
            <td><div class="day-number">3</div><div class="day-name">Пт</div><div class="notes">Напишите здесь</div></td>
            <td class="weekend"><div class="day-number">4</div><div class="day-name">Сб</div><div class="notes">Напишите здесь</div></td>
            <td class="weekend"><div class="day-number">5</div><div class="day-name">Вс</div><div class="notes">Напишите здесь</div></td>
        </tr>
        <tr>
            <td><div class="day-number">6</div><div class="day-name">Пн</div><div class="notes">Напишите здесь</div></td>
            <td><div class="day-number">7</div><div class="day-name">Вт</div><div class="notes">Напишите здесь</div></td>
            <td><div class="day-number">8</div><div class="day-name">Ср</div><div class="notes">Напишите здесь</div></td>
            <td><div class="day-number">9</div><div class="day-name">Чт</div><div class="notes">Напишите здесь</div></td>
            <td><div class="day-number">10</div><div class="day-name">Пт</div><div class="notes">Напишите здесь</div></td>
            <td class="weekend"><div class="day-number">11</div><div class="day-name">Сб</div><div class="notes">Напишите здесь</div></td>
            <td class="weekend"><div class="day-number">12</div><div class="day-name">Вс</div><div class="notes">Напишите здесь</div></td>
        </tr>
        <tr>
            <td><div class="day-number">13</div><div class="day-name">Пн</div><div class="notes">Напишите здесь</div></td>
            <td><div class="day-number">14</div><div class="day-name">Вт</div><div class="notes">Напишите здесь</div></td>
            <td><div class="day-number">15</div><div class="day-name">Ср</div><div class="notes">Напишите здесь</div></td>
            <td><div class="day-number">16</div><div class="day-name">Чт</div><div class="notes">Напишите здесь</div></td>
            <td><div class="day-number">17</div><div class="day-name">Пт</div><div class="notes">Напишите здесь</div></td>
            <td class="weekend"><div class="day-number">18</div><div class="day-name">Сб</div><div class="notes">Напишите здесь</div></td>
            <td class="weekend"><div class="day-number">19</div><div class="day-name">Вс</div><div class="notes">Напишите здесь</div></td>
        </tr>
        <tr>
            <td><div class="day-number">20</div><div class="day-name">Пн</div><div class="notes">Напишите здесь</div></td>
            <td><div class="day-number">21</div><div class="day-name">Вт</div><div class="notes">Напишите здесь</div></td>
            <td><div class="day-number">22</div><div class="day-name">Ср</div><div class="notes">Напишите здесь</div></td>
            <td><div class="day-number">23</div><div class="day-name">Чт</div><div class="notes">Напишите здесь</div></td>
            <td><div class="day-number">24</div><div class="day-name">Пт</div><div class="notes">Напишите здесь</div></td>
            <td class="weekend"><div class="day-number">25</div><div class="day-name">Сб</div><div class="notes">Напишите здесь</div></td>
            <td class="weekend"><div class="day-number">26</div><div class="day-name">Вс</div><div class="notes">Напишите здесь</div></td>
        </tr>
        <tr>
            <td><div class="day-number">27</div><div class="day-name">Пн</div><div class="notes">Напишите здесь</div></td>
            <td><div class="day-number">28</div><div class="day-name">Вт</div><div class="notes">Напишите здесь</div></td>
            <td><div class="day-number">29</div><div class="day-name">Ср</div><div class="notes">Напишите здесь</div></td>
            <td><div class="day-number">30</div><div class="day-name">Чт</div><div class="notes">Напишите здесь</div></td>
            <td><div class="day-number">31</div><div class="day-name">Пт</div><div class="notes">Напишите здесь</div></td>
            <td colspan="2"></td>
        </tr>
    </table>
</body>
</html>
"""

# Создаем PDF из HTML
HTML(string=html_content).write_pdf('/Users/yuriy/Desktop/Проекты/Стилва/Календарь_Январь_2026.pdf')
print('✅ PDF создан с русским текстом!')
