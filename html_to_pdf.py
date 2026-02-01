import subprocess
import os

html_file = '/Users/yuriy/Desktop/Проекты/Стилва/Календарь_Январь_2026_для_печати.html'
pdf_file = '/Users/yuriy/Desktop/Проекты/Стилва/Календарь_Январь_2026.pdf'

# Используем Safari для печати в PDF
script = f"""
tell application "Safari"
    open "{html_file}"
    delay 2
    print document 1 to file "{pdf_file}"
    quit
end tell
"""

with open('/tmp/print_script.applescript', 'w') as f:
    f.write(script)

result = subprocess.run(['osascript', '/tmp/print_script.applescript'], capture_output=True)
if result.returncode == 0:
    print(f'✅ PDF создан: {pdf_file}')
else:
    print(f'❌ Ошибка: {result.stderr.decode()}')
