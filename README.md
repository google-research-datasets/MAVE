# MAVE: : A Product Dataset for Multi-source Attribute Value Extraction
The dataset contains 3 million attribute-value annotations across 1257 unique categories on 2.2 million cleaned Amazon product profiles. It is a large, multi-sourced, diverse dataset for product attribute extraction study.

The dataset is in [JSON Lines](https://jsonlines.org/) format, where each line is a json object with the following schema:
```
{
   "id": <product id>,
   "category": <category name>,
   "paragraphs": [
      {
         "text": <paragraph text>,
         "source": <paragraph source>
      },
      ...
   ],
   "attributes": [
      {
         "key": <attribute name>,
         "evidences": [
            {
               "value": <attribute value>,
               "pid": <the paragraph id where the attribute value come from>,
               "begin": <the begin index of the attribute value in the paragraph>,
               "end": <the end index (exclusive) of the attribute value in the paragraph>
            },
            ...
         ]
      },
      ...
   ]
}
```
The product id is exactly the ASIN number in the `All_Amazon_Meta.json` file in the [Amazon Review Data (2018)](https://nijianmo.github.io/amazon/index.html). In this repo, we don't store `paragraphs`, instead we only store the labels. To obtain the full version dataset, we suggest to first request the [Amazon Review Data (2018)](https://nijianmo.github.io/amazon/index.html) to obtain the `All_Amazon_Meta.json` file, then run our binary to clean its product profile and join with the labels. The detailed instructions are in below.

A concrete example of the product profile and its attributes is shown as follows
```
{
   "id":"B0002H0A3S",
   "category":"Guitar Strings",
   "paragraphs":[
      {
         "text":"D'Addario EJ26 Phosphor Bronze Acoustic Guitar Strings, Custom Light, 11-52",
         "source":"title"
      },
      {
         "text":".011-.052 Custom Light Gauge Acoustic Guitar Strings, Phosphor Bronze",
         "source":"description"
      },
      ...
   ],
   "attributes":[
      {
         "key":"Core Material",
         "evidences":[
            {
               "value":"Bronze Acoustic",
               "pid":0,
               "begin":24,
               "end":39
            },
            ...
         ]
      },
      {
         "key":"Winding Material",
         "evidences":[
            {
               "value":"Phosphor Bronze",
               "pid":0,
               "begin":15,
               "end":30
            },
            ...
         ]
      },
      {
         "key":"Gauge",
         "evidences":[
            {
               "value":"Light",
               "pid":0,
               "begin":63,
               "end":68
            },
            {
               "value":"Light Gauge",
               "pid":1,
               "begin":17,
               "end":28
            },
            ...
         ]
      }
   ]
}
```

In addition to positive examples, we also provide a set of negative examples, i.e. product profiles without any attribute. The overall statistics of the positive and negative sets are as follows
| Counts                            | Positives   | Negatives   |
| :-------------------------------: | :---------: | :---------: |
| # products                        | 2226509     | 1248009     |
| # product-attribute pairs         | 2987151     | 1780428     |
| # products with 1-2 attributes    | 2102927     | 1140561     |
| # products with 3-5 attributes    | 121897      | 99896       |
| # products with >=6 attributes    | 1685        | 7552        |
| # unique categories               | 1257        | 1114        |
| # unique attributes               | 705         | 693         |
| # unique category-attribute pairs | 2535        | 2305        |

## Creating the full version of the dataset
In this repo, we only open source the labels of the MAVE dataset and the code to deterministically clean the original `All_Amazon_Meta.json` file in the [Amazon Review Data (2018)](https://nijianmo.github.io/amazon/index.html) to product profiles, and join with the labels to generate the full version of the MAVE dataset. After this process, the attribute values, paragraph ids and begin/end span indices will be consistent with the cleaned product profiles.

#### Step 1
Gain access to the [Amazon Review Data (2018)](https://nijianmo.github.io/amazon/index.html) and download the 
```
All_Amazon_Meta.json
```
file to the folder of this repo.
#### Step 2
Run the provided script
```
./clean_amazon_product_metadata_main.sh
```
, which will clean the Amazon metadata to product profiles and join with the positive and negative labels in the `labels/` folder.
The output full version of the MAVE dataset will be stored in the `reproduce/` folder.

The script runs the `clean_amazon_product_metadata_main.py` binary using an [apache beam](https://beam.apache.org/) pipeline. The binary will run on a single CPU core, but distributed setup can be enabled by changing pipeline options. The binary contains utils to clean the Amazon metadata to product profiles and join with labels. The pipeline will finish within a few hours on a Intel Xeon 3GHz CPU core.
