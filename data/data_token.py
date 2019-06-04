# -*- coding: utf-8 -*-
import jieba
import sys,glob,os,csv,re
if sys.getdefaultencoding() != 'utf-8':
	reload(sys)
	sys.setdefaultencoding('utf-8')


class Token_Config(object):
	token_vocab = "token_vocab/token_vocab.txt"
	data_path = "train"
	stopwords_path = "token_vocab/stopwords.txt"


class Fiter(object):
	"""docstring for Fiter"""
	def __init__(self):
		super(Fiter, self).__init__()
		# 规则的顺序不能乱，否则会出错
		self.rules = []
		# self.rules.append({r'^\d+\.\d+$':"TAG_NUM"})
		# self.rules.append({r'^\d+$':"TAG_NUM"})

	def start(self,sentence):
		# return sentence
		for rule in self.rules:
			for pattern,repl in rule.items():
				sentence = re.sub(pattern, repl, sentence, count=0, flags=0)
		return sentence

	def pre_process(self,sentence,lable=False):
		SENTENCE_START = '<s>'
		SENTENCE_END = '</s>'
		START_DECODING = '[START]'
		STOP_DECODING = '[STOP]'
		# sentences = re.split('。|！|\!|\.|？|\?',sentence)
		# sentence = ' '.join(["%s %s %s" % (SENTENCE_START, sent.strip(), SENTENCE_END) for sent in sentences if sent != ""])
		# sentence = "%s %s %s" % (START_DECODING, sentence, STOP_DECODING)

		if lable:
			sentence = "%s %s %s" % (SENTENCE_START, sentence, SENTENCE_END)
		else:
			sentence = "%s %s %s" % (START_DECODING, sentence, STOP_DECODING)
		return sentence

		
class TokenVocab(object):
	"""docstring for TokenVocab"""
	def __init__(self, cfg):
		super(TokenVocab, self).__init__()
		self.cfg = cfg
		self.rm_cache()
		jieba.load_userdict(cfg.token_vocab)
		self.fiter = Fiter()

	def rm_cache(self):
		cmd = "rm -rf /tmp/jieba.cache"
		os.system(cmd)
	# 对句子进行分词  
	def seg_sentence(self,sentence,lable=False): 
		sentence_seged = jieba.cut(sentence.strip())
		# 这里加载停用词的路径
		stopwords = self.stopwordslist(self.cfg.stopwords_path)
		outstr = '' 
		for word in sentence_seged:
			if word not in stopwords:
				word = self.fiter.start(word)
				if word != '\t':
					outstr += word
					outstr += " "
		outstr = outstr.strip()
		outstr = self.fiter.pre_process(outstr,lable)
		return outstr

	# 创建停用词list
	def stopwordslist(self,filepath):
		if not os.path.exists(filepath):
			return []
		stopwords = [line.strip() for line in open(filepath, mode="r",).readlines()] 
		return stopwords

	# 写入文件
	def write_tokens_file(self,filepath):
		with open(filepath,"r") as f:
			token_name = filepath + ".tokens"
			w = open(token_name,'w')
			reader = csv.reader(f)
			for row in reader:
				if len(row) < 4:
					continue
				label = row[0]
				sentence = row[1]
				id = row[2]
				aid = row[3]
				tokens_sentence = self.seg_sentence(sentence,False)
				tokens_label = self.seg_sentence(label,True)
				d = "label<=>"+tokens_label + "\t|separator|\t" + "sentence<=>" + tokens_sentence
				d = d + "\t|separator|\t" + "id<=>" + id + "\t|separator|\t" + "aid<=>" + aid + "\n"
				w.write(d)
			w.close()

	def start(self):
		for f_name in glob.glob(os.path.join(self.cfg.data_path, "*.csv")):
			self.write_tokens_file(f_name)

	def general(self):
		# for f_name in glob.glob(os.path.join("train", "*.csv")):
		# 	self.write_tokens_file(f_name)
		# for f_name in glob.glob(os.path.join("eval", "*.csv")):
		# 	self.write_tokens_file(f_name)
		for f_name in glob.glob(os.path.join("test", "*.csv")):
			self.write_tokens_file(f_name)


	

def main():
	cfg = Token_Config()
	t = TokenVocab(cfg)
	t.general()

if __name__ == '__main__':
	main()