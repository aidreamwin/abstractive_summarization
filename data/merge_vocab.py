# -*- coding: utf-8 -*-

def read_file(data,file_path):
	with open(file_path,"r") as f:
		for line in f:
			pieces = line.split()
			try:
				if len(pieces) < 2:
					if data.get(pieces[0]) == None:
						data[pieces[0]] = 1
					else:
						data[pieces[0]] += 1
				else:
					if data.get(pieces[0]) == None:
						data[pieces[0]] = 1
					else:
						data[pieces[0]] += int(pieces[1])
			except Exception as e:
				print(e,pieces)
				continue

	return data


def main():
	data = {}
	data = read_file(data,"token_vocab/token_vocab.txt")
	data = read_file(data,"token_vocab/voc_dict_senc.txt")
	data = read_file(data,"token_vocab/voc_dict_label.txt")

	save_path = "token_vocab/merge_tmp.txt"
	voc_sort = sorted(data.items(), key=lambda item:item[1] , reverse=True)
	with open(save_path,"w") as f:
		for d_v in voc_sort:
			f.write("{} {}\n".format(d_v[0],d_v[1]))

if __name__ == '__main__':
	main()