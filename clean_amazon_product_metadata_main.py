"""Deterministically cleans Amazon Product Metadata and joins with labels."""
import copy
import itertools
import json
import re
from typing import Any, Iterator, Mapping, Sequence, Tuple, Union

from absl import app
from absl import flags
from absl import logging
import apache_beam as beam
import bs4


_INPUT_AMAZON_PRODUCT_METADATA_JSON_FILENAME = flags.DEFINE_string(
    'input_amazon_product_metadata_json_filename',
    default=None,
    help='The input JSON file name for the Amazon Product Metadata.',
    required=True)

_INPUT_ATTRIBUTE_LABELS_JSON_LINES_FILENAME = flags.DEFINE_string(
    'input_attribute_labels_json_lines_filename',
    default=None,
    help='The input JSON Lines file name for attribute labels.',
    required=True)

_OUTPUT_JSON_LINES_FILENAME = flags.DEFINE_string(
    'output_json_lines_filename',
    default=None,
    help='The output JSON Lines filename after cleaning and joining labels.',
    required=True)

_OUTPUT_JSON_LINES_STAT_FILENAME = flags.DEFINE_string(
    'output_json_lines_stat_filename',
    default=None,
    help='The output JSON Lines statistics filename.')

# ID of the product, e.g. 0000031852.
_ASIN = 'asin'
# Name of the product.
_TITLE = 'title'
# Description of the product, including Bullet-point descriptions under title.
_DESCRIPTION = 'description'
# Bullet-point format features of the product
_FEATURE = 'feature'
# Price in US dollars (at time of crawl)
_PRICE = 'price'
# Brand name
_BRAND = 'brand'
_SOURCES = (_TITLE, _DESCRIPTION, _FEATURE, _PRICE, _BRAND)

# The type for a JSON example.
_JsonObject = Mapping[str, Union[str, Sequence[Any]]]


class JoinWithLabelsFn(beam.DoFn):
  """DoFn to join Amazon Product Metadata with attribute labels."""

  def process(self, json_example: _JsonObject,
              labels_by_id: Mapping[str, _JsonObject], *args,
              **kwargs) -> Iterator[Tuple[_JsonObject, _JsonObject]]:
    labels = labels_by_id.get(json_example.get(_ASIN, ''))
    if not labels:
      return
    yield json_example, copy.deepcopy(labels)


class ConvertSourcesFn(beam.DoFn):
  """DoFn to convert Amazon Product Metadata sources."""

  def process(self, element: Tuple[_JsonObject, _JsonObject], *args,
              **kwargs) -> Iterator[_JsonObject]:
    json_example, labels = element
    paragraphs = list(
        itertools.chain.from_iterable(
            self._create_paragraphs(source, json_example)
            for source in _SOURCES))

    yield {
        'paragraphs': paragraphs,
        **(copy.deepcopy(labels)),
    }

  def _create_paragraphs(
      self, source: str,
      json_example: _JsonObject) -> Iterator[Mapping[str, str]]:
    """Yields sources from a json_example."""
    data = json_example.get(source, [])

    if isinstance(data, str):
      texts = [data]
    elif isinstance(data, list):
      texts = data
    else:
      texts = []
      logging.info('Invalid json format: %r in %r, not a string or a list.',
                   source, json_example)

    paragraphs = [
        dict(text=text.strip(), source=source)
        for text in texts
        if text.strip()
    ]

    yield from paragraphs


def is_css(text: str) -> bool:
  """Returns whether the text is detected as purely CSS."""
  num_tokens = 0
  num_css_elements = 0
  for token in text.split():
    num_tokens += 1
    if (token.startswith(('.', '#', 'div')) or 'px' in token or
        len(re.findall('-', token)) > 1):
      num_css_elements += 1

  if num_css_elements / num_tokens > 0.3 and num_css_elements > 20:
    return True
  return False


def is_html(text: str) -> bool:
  """Returns whether the text is detected as HTML."""
  if '<a href' in text:
    return True
  if 'javascript:' in text:
    return True
  if 'background-color:' in text:
    return True
  if 'background-image:' in text:
    return True
  if ' li:' in text:
    return True
  if '.aloha' in text:
    return True

  return False


def remove_tags(html: str) -> str:
  """Removes HTML style and script tags.

  Following
  https://www.geeksforgeeks.org/remove-all-style-scripts-and-html-tags-using-beautifulsoup/

  Args:
    html: A string of HTML.

  Returns:
    Cleaned text content of HTML.
  """

  # parses html content
  soup = bs4.BeautifulSoup(html, 'html.parser')

  for data in soup(['style', 'script']):
    # Removes tags
    data.decompose()

  # returns data by retrieving the tag content
  return ' '.join(soup.stripped_strings)


class CleanParagraphsFn(beam.DoFn):
  """DoFn to clean paragraphs."""

  def __init__(self) -> None:
    self._num_paragraph = beam.metrics.Metrics.counter(self.__class__,
                                                       'num-paragraphs')
    self._unformatted_title = beam.metrics.Metrics.counter(
        self.__class__, 'num-paragraph-unformatted-title')
    self._unicode_issue_before_css = beam.metrics.Metrics.counter(
        self.__class__, 'num-paragraph-unicode-issue-before-css-removal')
    self._html_cleaned = beam.metrics.Metrics.counter(
        self.__class__, 'num-paragraph-html-cleaned')
    self._whitespace_removed = beam.metrics.Metrics.counter(
        self.__class__, 'num-paragraph-empty-after-whitespace-removed')
    self._html_after_clean = beam.metrics.Metrics.counter(
        self.__class__, 'num-paragraph-html-after-all-clean')
    self._css_removed = beam.metrics.Metrics.counter(
        self.__class__, 'num-paragraph-is-css-removed')
    self._unicode_issue_after_clean = beam.metrics.Metrics.counter(
        self.__class__, 'num-paragraph-unicode-issue-after-all-clean')
    self._no_title = beam.metrics.Metrics.counter(self.__class__,
                                                  'num-document-no-title')
    self._num_output_documents = beam.metrics.Metrics.counter(
        self.__class__, 'num-output-documents')

  def process(self, json_example: _JsonObject, *args,
              **kwargs) -> Iterator[_JsonObject]:
    paragraphs = []
    for paragraph in json_example['paragraphs']:
      self._num_paragraph.inc()
      text = paragraph['text']
      if paragraph['source'] == 'title' and 'getTime' in text:
        self._unformatted_title.inc()
        continue

      # Unicode clean before clean.
      text_bytes = text.encode('utf-8', 'ignore')
      text_recovered = text_bytes.decode('utf-8')
      if text_recovered != text:
        self._unicode_issue_before_css.inc()

      # HTML clean.
      text_html_cleaned = remove_tags(text_recovered)
      if text_html_cleaned != text_recovered:
        self._html_cleaned.inc()

      # Whitespace clean.
      text_space_cleaned = ' '.join(text_html_cleaned.split())
      if not text_space_cleaned:
        self._whitespace_removed.inc()
        continue

      if is_html(text_space_cleaned):
        self._html_after_clean.inc()
        continue

      # CSS removal.
      if is_css(text_space_cleaned):
        self._css_removed.inc()
        continue

      # Unicode clean after clean.
      text_space_cleaned_bytes = text_space_cleaned.encode('utf-8', 'ignore')
      text_space_cleaned_recovered = text_space_cleaned_bytes.decode('utf-8')
      if text_space_cleaned_recovered != text_space_cleaned:
        self._unicode_issue_after_clean.inc()
        logging.info(text_space_cleaned)

      paragraphs.append(
          dict(text=text_space_cleaned_recovered, source=paragraph['source']))

    for paragraph in paragraphs:
      if paragraph['source'] == 'title':
        break
    else:
      self._no_title.inc()
      return

    self._num_output_documents.inc()
    yield {
        'id': json_example['id'],
        'category': json_example['category'],
        'paragraphs': paragraphs,
        'attributes': json_example['attributes'],
    }


def pipeline(root):
  """Beam pipeline to run."""
  labels = (
      root
      | 'ReadJSON_Labels' >> beam.io.textio.ReadFromText(
          _INPUT_ATTRIBUTE_LABELS_JSON_LINES_FILENAME.value)
      | 'JSONLoads_Labels' >> beam.Map(json.loads)
      | 'KeyById' >> beam.Map(lambda x: (x['id'], x)))

  output_examples = (
      root
      | 'ReadJSON_AmazonMetadata' >> beam.io.textio.ReadFromText(
          _INPUT_AMAZON_PRODUCT_METADATA_JSON_FILENAME.value)
      | 'JSONLoads_AmazonMetadata' >> beam.Map(json.loads)
      | 'JoinWithLabels' >> beam.ParDo(
          JoinWithLabelsFn(), labels_by_id=beam.pvalue.AsDict(labels))
      | 'ConvertSources' >> beam.ParDo(ConvertSourcesFn())
      | 'CleanParagraphs' >> beam.ParDo(CleanParagraphsFn())
      | 'GroupByASIN' >> beam.GroupBy(lambda x: x['id'])
      | 'DedupeASIN' >> beam.Map(lambda x: list(x[1])[0])  # It is Determinstic.
  )

  _ = (
      output_examples
      | 'JSONDumps' >> beam.Map(json.dumps)
      | 'WriteToJSONLine' >> beam.io.WriteToText(
          _OUTPUT_JSON_LINES_FILENAME.value,
          shard_name_template='',  # To force unsharded output.
      ))

  if _OUTPUT_JSON_LINES_STAT_FILENAME.value:
    _ = (
        output_examples
        | 'CountOutputExamples' >> beam.combiners.Count.Globally()
        | beam.Map(lambda x: json.dumps(x, indent=2))
        | beam.io.WriteToText(
            _OUTPUT_JSON_LINES_STAT_FILENAME.value,
            shard_name_template='',  # To force unsharded output.
        ))


def main(unused_argv: Sequence[str]) -> None:
  # To enable distributed workflows, follow instructions at
  # https://beam.apache.org/documentation/programming-guide/
  # to set pipeline options.
  with beam.Pipeline() as p:
    pipeline(p)


if __name__ == '__main__':
  app.run(main)
