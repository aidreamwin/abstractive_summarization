cd ../src
python run_summarization.py \
--mode=eval \
--data_path=../data/test/test.bin \
--eval_data_path=../data/eval/eval.bin \
--vocab_path=../data/token_vocab/merge_vocab.txt \
--log_root=../log \
--exp_name=base_dropout_0.85 \
--batch_size=20 \
--use_temporal_attention=False \
--intradecoder=False \
--eta=0 \
--rl_training=True \
--lr=0.15 \
--sampling_probability=1 \
--fixed_eta=True \
--scheduled_sampling=True \
--fixed_sampling_probability=True \
--greedy_scheduled_sampling=True