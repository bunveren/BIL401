import csv
import os

input_csv_path = "data/train_cleaned.csv" 
output_csv_path = "data/train_properly_quoted.csv" 

output_dir = os.path.dirname(output_csv_path)
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

print(f"'{input_csv_path}' dosyasi okunuyor ve yeniden yaziliyor (duzgun tirnaklama)...")

try:
    with open(input_csv_path, 'r', encoding='utf-8', newline='') as infile:
        reader = csv.reader(infile, delimiter=',', quotechar='"')
        with open(output_csv_path, 'w', encoding='utf-8', newline='') as outfile:
            writer = csv.writer(outfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            header = next(reader)
            writer.writerow(header)
            for row in reader:
                 writer.writerow(row)

    print(f"Dosya basariyla yeniden yazildi: '{output_csv_path}'")

except FileNotFoundError:
    print(f"HATA: '{input_csv_path}' dosyasi bulunamadi.")
except Exception as e:
    print(f"Dosyayi yeniden yazarken bir hata olustu: {e}")