# Run locally
AMAZON_META_DATA="./All_Amazon_Meta.json"

python3 clean_amazon_product_metadata_main.py \
--input_amazon_product_metadata_json_filename="${AMAZON_META_DATA}" \
--input_attribute_labels_json_lines_filename='./labels/mave_positives_labels.jsonl' \
--output_json_lines_filename='./reproduce/mave_positives.jsonl' \
--output_json_lines_stat_filename='./reproduce/mave_positives_counts'

python3 clean_amazon_product_metadata_main.py \
--input_amazon_product_metadata_json_filename="${AMAZON_META_DATA}" \
--input_attribute_labels_json_lines_filename='./labels/mave_negatives_labels.jsonl' \
--output_json_lines_filename='./reproduce/mave_negatives.jsonl' \
--output_json_lines_stat_filename='./reproduce/mave_negatives_counts'
