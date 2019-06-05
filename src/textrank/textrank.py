# -*- coding: utf-8 -*-

from .textrank4zh import TextRank4Keyword, TextRank4Sentence

import sys
sys.path.append("..")
from mlog.mlog import mlog

class TextRank(object):
	"""docstring for TextRank"""
	def __init__(self):
		self.tr4w_word = TextRank4Keyword()
		self.tr4s_sent = TextRank4Sentence()

	def filter(self,text):
		if text == "" or len(text) < 1:
			mlog.error("非法参数，参数不许为空")
			raise Exception("text is null")


	# py2中text必须是utf8编码的str或者unicode对象，
	# py3中必须是utf8编码的bytes或者str对象
	def get_keywords(self, text, keywords_num=5,window=2):
		try:
			self.filter(text)
		except Exception as e:
			return []
		self.tr4w_word.analyze(text=text, lower=True, window=window)
		items = self.tr4w_word.get_keywords(keywords_num, word_min_len=2)
		# item.word, item.weight
		return items

	def get_key_phrases(self, text, keywords_num=3,window=2):
		try:
			self.filter(text)
		except Exception as e:
			return []
		self.tr4w_word.analyze(text=text, lower=True, window=window)
		phrases = self.tr4w_word.get_keyphrases(keywords_num=keywords_num, min_occur_num= 2)
		return phrases

	def get_key_sentences(self, text, num=3):
		try:
			self.filter(text)
		except Exception as e:
			return []
		# item.index, item.weight, item.sentence
		self.tr4s_sent.analyze(text=text, lower=True, source = 'all_filters')
		sentences = self.tr4s_sent.get_key_sentences(num=num)
		return sentences

if __name__ == '__main__':
	tr = TextRank()
	text = "美国是想让中国的核力量发展到他们一样的水平吗？亦或者是美国减到中国核武器的水平？不得不说我国这次的回应十分霸气。我国的核力量可以说是中美俄三国中常备核弹头最少的国家了，"
	# text = "这也带动了美国民众的反战情绪，这些可是美军的财神爷，美国政府自然得罪不起，因此在尼克松竞选时就承诺结束越南战争，即便打了20年没成功，也还是灰溜溜的回国了。对美军来说，"
	kws = tr.get_keywords(text)

	for kw in kws:
		print(kw)