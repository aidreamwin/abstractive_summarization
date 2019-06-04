cd ../data
# python cut.py
python data_token.py
python data_convert_example.py  --command text_to_binary --in_file train/train.csv.tokens  --out_file train/train.bin
python data_convert_example.py  --command text_to_binary --in_file eval/eval.csv.tokens  --out_file eval/eval.bin
python data_convert_example.py  --command text_to_binary --in_file test/test.csv.tokens  --out_file test/test.bin

python data_convert_example.py  --in_file train/train.bin  --out_file train/train.txt
python data_convert_example.py  --in_file eval/eval.bin  --out_file eval/eval.txt
python data_convert_example.py  --in_file test/test.bin  --out_file test/test.txt
