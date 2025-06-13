import os

input_csv_path = "data/train.csv/train.csv"
output_csv_path = "data/train_cleaned.csv" 
output_dir = os.path.dirname(output_csv_path)
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

print(f"'{input_csv_path}' dosyasi okunuyor ve temizleniyor...")

try:
    with open(input_csv_path, 'r', encoding='utf-8', newline='') as infile, \
         open(output_csv_path, 'w', encoding='utf-8', newline='') as outfile:
        header = infile.readline()
        cleaned_header = header.strip() 
        outfile.write(cleaned_header + '\n') 
        for line in infile:
            cleaned_line = line.strip()
            if cleaned_line:
                 outfile.write(cleaned_line + '\n') 

    print(f"Dosya basariyla temizlendi ve '{output_csv_path}' olarak kaydedildi.")

except FileNotFoundError:
    print(f"HATA: '{input_csv_path}' dosyasi bulunamadi.")
except Exception as e:
    print(f"Dosya temizleme sirasinda bir hata olustu: {e}")