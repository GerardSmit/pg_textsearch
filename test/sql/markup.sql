CREATE EXTENSION IF NOT EXISTS pg_textsearch;

SET enable_seqscan = off;

-- ============================================================
-- 1. Default plain behavior (backward compatibility)
-- ============================================================

DROP TABLE IF EXISTS markup_plain CASCADE;
CREATE TABLE markup_plain (id int PRIMARY KEY, content text);
INSERT INTO markup_plain VALUES
  (1, 'Hello world'),
  (2, 'The quick brown fox');
CREATE INDEX markup_plain_idx ON markup_plain USING bm25(content)
  WITH (text_config = 'english');

SELECT id FROM markup_plain
  ORDER BY content <@> to_bm25query('hello', 'markup_plain_idx') LIMIT 1;

SELECT id FROM markup_plain
  ORDER BY content <@> to_bm25query('fox', 'markup_plain_idx') LIMIT 1;

-- ============================================================
-- 2. HTML normalization
-- ============================================================

DROP TABLE IF EXISTS markup_html CASCADE;
CREATE TABLE markup_html (id int PRIMARY KEY, content text);
INSERT INTO markup_html VALUES
  (1, '<p>Hello <b>world</b> &amp; friends</p>'),
  (2, '<div class="main"><a href="https://example.com">click here</a></div>'),
  (3, '<script>alert("xss")</script><p>safe text only</p>'),
  (4, '&lt;strong&gt;Tom &amp; Jerry&lt;/strong&gt;');
CREATE INDEX markup_html_idx ON markup_html USING bm25(content)
  WITH (text_config = 'english', content_format = 'html');

-- Visible text searchable
SELECT id FROM markup_html
  ORDER BY content <@> to_bm25query('friend', 'markup_html_idx') LIMIT 1;

SELECT id FROM markup_html
  ORDER BY content <@> to_bm25query('click', 'markup_html_idx') LIMIT 1;

-- Script content stripped
SELECT id FROM markup_html
  ORDER BY content <@> to_bm25query('safe', 'markup_html_idx') LIMIT 1;

-- Tags/attributes not searchable (score = 0)
SELECT id, content <@> to_bm25query('div', 'markup_html_idx') AS score
  FROM markup_html WHERE id = 2;

-- URL from href not searchable
SELECT id, content <@> to_bm25query('example', 'markup_html_idx') AS score
  FROM markup_html WHERE id = 2;

-- Encoded tags: in HTML mode, &lt;strong&gt; decodes to <strong>
-- which is a tag and gets stripped, so 'strong' is NOT indexed
SELECT id, content <@> to_bm25query('strong', 'markup_html_idx') AS score
  FROM markup_html WHERE id = 4;

-- Entity decoding: Tom & Jerry visible text
SELECT id FROM markup_html
  ORDER BY content <@> to_bm25query('tom', 'markup_html_idx') LIMIT 1;

-- ============================================================
-- 3. Markdown normalization
-- ============================================================

DROP TABLE IF EXISTS markup_md CASCADE;
CREATE TABLE markup_md (id int PRIMARY KEY, content text);
INSERT INTO markup_md VALUES
  (1, '# Main Title

Some **bold** and *italic* text.'),
  (2, '[click here](https://example.com/path?q=foo)

Visit the site.'),
  (3, '```python
def hello():
    return "world"
```

Code above.'),
  (4, '- item one
- item two
- item three');
CREATE INDEX markup_md_idx ON markup_md USING bm25(content)
  WITH (text_config = 'english', content_format = 'markdown');

-- Visible text searchable
SELECT id FROM markup_md
  ORDER BY content <@> to_bm25query('bold', 'markup_md_idx') LIMIT 1;

SELECT id FROM markup_md
  ORDER BY content <@> to_bm25query('title', 'markup_md_idx') LIMIT 1;

-- Link label searchable
SELECT id FROM markup_md
  ORDER BY content <@> to_bm25query('click', 'markup_md_idx') LIMIT 1;

-- Code text searchable
SELECT id FROM markup_md
  ORDER BY content <@> to_bm25query('hello', 'markup_md_idx') LIMIT 1;

-- Link destination NOT searchable
SELECT id, content <@> to_bm25query('example', 'markup_md_idx') AS score
  FROM markup_md WHERE id = 2;

-- List items searchable
SELECT id FROM markup_md
  ORDER BY content <@> to_bm25query('item', 'markup_md_idx') LIMIT 1;

-- ============================================================
-- 4. Per-field content_format (multi-column)
-- ============================================================

DROP TABLE IF EXISTS markup_multi CASCADE;
CREATE TABLE markup_multi (id int PRIMARY KEY, title text, body text);
INSERT INTO markup_multi VALUES
  (1, 'Plain Title', '<p>HTML <b>body</b> &amp; text</p>'),
  (2, 'Another', '# Markdown heading with **bold** words');

CREATE INDEX markup_multi_idx ON markup_multi USING bm25(title, body)
  WITH (text_config = 'english', content_format = 'title:plain,body:html');

-- Title searchable (plain)
SELECT id FROM markup_multi
  ORDER BY (title, body) <@> to_bm25query('plain', 'markup_multi_idx') LIMIT 1;

-- Body visible text searchable (HTML stripped)
SELECT id FROM markup_multi
  ORDER BY (title, body) <@> to_bm25query('body', 'markup_multi_idx') LIMIT 1;

-- ============================================================
-- 5. Bare format applies to all columns
-- ============================================================

DROP TABLE IF EXISTS markup_bare CASCADE;
CREATE TABLE markup_bare (id int PRIMARY KEY, title text, body text);
INSERT INTO markup_bare VALUES
  (1, '# MD Title', '# MD Body with **bold** words');
CREATE INDEX markup_bare_idx ON markup_bare USING bm25(title, body)
  WITH (text_config = 'english', content_format = 'markdown');

SELECT id FROM markup_bare
  ORDER BY (title, body) <@> to_bm25query('bold', 'markup_bare_idx') LIMIT 1;

-- ============================================================
-- 6. Two-column: plain + markdown
-- ============================================================

DROP TABLE IF EXISTS markup_2c_pm CASCADE;
CREATE TABLE markup_2c_pm (id int PRIMARY KEY, title text, body text);
INSERT INTO markup_2c_pm VALUES
  (1, 'Plain Title', '## Markdown body with **bold** words'),
  (2, 'Another Plain', 'No markdown here');
CREATE INDEX markup_2c_pm_idx ON markup_2c_pm USING bm25(title, body)
  WITH (text_config = 'english', content_format = 'title:plain,body:markdown');

-- Title searchable (plain)
SELECT id FROM markup_2c_pm
  ORDER BY (title, body) <@> to_bm25query('plain', 'markup_2c_pm_idx') LIMIT 1;

-- Body visible text searchable (markdown stripped)
SELECT id FROM markup_2c_pm
  ORDER BY (title, body) <@> to_bm25query('bold', 'markup_2c_pm_idx') LIMIT 1;

-- ============================================================
-- 7. Two-column: html + markdown
-- ============================================================

DROP TABLE IF EXISTS markup_2c_hm CASCADE;
CREATE TABLE markup_2c_hm (id int PRIMARY KEY, title text, body text);
INSERT INTO markup_2c_hm VALUES
  (1, '<b>HTML</b> Title &amp; More', '## Markdown body with **bold** words'),
  (2, '<em>Another</em> HTML', '[link text](https://example.com)');
CREATE INDEX markup_2c_hm_idx ON markup_2c_hm USING bm25(title, body)
  WITH (text_config = 'english', content_format = 'title:html,body:markdown');

-- HTML title searchable, tags stripped
SELECT id FROM markup_2c_hm
  ORDER BY (title, body) <@> to_bm25query('html', 'markup_2c_hm_idx') LIMIT 1;

-- Markdown body searchable
SELECT id FROM markup_2c_hm
  ORDER BY (title, body) <@> to_bm25query('bold', 'markup_2c_hm_idx') LIMIT 1;

-- Link destination NOT searchable
SELECT id, (title, body) <@> to_bm25query('example', 'markup_2c_hm_idx') AS score
  FROM markup_2c_hm WHERE id = 2;

-- ============================================================
-- 8. Two-column: markdown + html (reversed order)
-- ============================================================

DROP TABLE IF EXISTS markup_2c_mh CASCADE;
CREATE TABLE markup_2c_mh (id int PRIMARY KEY, summary text, content text);
INSERT INTO markup_2c_mh VALUES
  (1, '**Bold** summary with [link](http://x.com)', '<div>HTML content &amp; stuff</div>'),
  (2, 'Plain summary text', '<p>More <b>HTML</b> here</p>');
CREATE INDEX markup_2c_mh_idx ON markup_2c_mh USING bm25(summary, content)
  WITH (text_config = 'english', content_format = 'summary:markdown,content:html');

-- Markdown summary searchable
SELECT id FROM markup_2c_mh
  ORDER BY (summary, content) <@> to_bm25query('bold', 'markup_2c_mh_idx') LIMIT 1;

-- HTML content searchable
SELECT id FROM markup_2c_mh
  ORDER BY (summary, content) <@> to_bm25query('stuff', 'markup_2c_mh_idx') LIMIT 1;

-- Link destination in markdown NOT searchable
SELECT id, (summary, content) <@> to_bm25query('x.com', 'markup_2c_mh_idx') AS score
  FROM markup_2c_mh WHERE id = 1;

-- ============================================================
-- 9. Two-column: bare html (all columns)
-- ============================================================

DROP TABLE IF EXISTS markup_2c_bh CASCADE;
CREATE TABLE markup_2c_bh (id int PRIMARY KEY, title text, body text);
INSERT INTO markup_2c_bh VALUES
  (1, '<b>HTML</b> title', '<p>HTML body &amp; text</p>');
CREATE INDEX markup_2c_bh_idx ON markup_2c_bh USING bm25(title, body)
  WITH (text_config = 'english', content_format = 'html');

-- Both columns normalized as HTML
SELECT id FROM markup_2c_bh
  ORDER BY (title, body) <@> to_bm25query('html', 'markup_2c_bh_idx') LIMIT 1;

SELECT id, (title, body) <@> to_bm25query('amp', 'markup_2c_bh_idx') AS score
  FROM markup_2c_bh WHERE id = 1;

-- ============================================================
-- 10. Three-column: plain + html + markdown
-- ============================================================

DROP TABLE IF EXISTS markup_3c CASCADE;
CREATE TABLE markup_3c (
  id int PRIMARY KEY,
  title text,
  summary text,
  body text
);
INSERT INTO markup_3c VALUES
  (1,
   'Plain Title Text',
   '<p>HTML summary with <b>bold</b> &amp; entities</p>',
   '## Markdown body

Some **formatted** text with [a link](https://example.com).'),
  (2,
   'Another Plain',
   '<script>evil()</script><p>Safe summary</p>',
   '- list item alpha
- list item beta');
CREATE INDEX markup_3c_idx ON markup_3c USING bm25(title, summary, body)
  WITH (text_config = 'english',
        content_format = 'title:plain,summary:html,body:markdown');

-- Plain title
SELECT id FROM markup_3c
  ORDER BY (title, summary, body) <@> to_bm25query('plain', 'markup_3c_idx')
  LIMIT 1;

-- HTML summary visible text
SELECT id FROM markup_3c
  ORDER BY (title, summary, body) <@> to_bm25query('bold', 'markup_3c_idx')
  LIMIT 1;

-- HTML script content stripped
SELECT id FROM markup_3c
  ORDER BY (title, summary, body) <@> to_bm25query('safe', 'markup_3c_idx')
  LIMIT 1;

-- Markdown body visible text
SELECT id FROM markup_3c
  ORDER BY (title, summary, body) <@> to_bm25query('format', 'markup_3c_idx')
  LIMIT 1;

-- Markdown link destination NOT searchable
SELECT id, (title, summary, body) <@> to_bm25query('example', 'markup_3c_idx') AS score
  FROM markup_3c WHERE id = 1;

-- Markdown list items searchable
SELECT id FROM markup_3c
  ORDER BY (title, summary, body) <@> to_bm25query('alpha', 'markup_3c_idx')
  LIMIT 1;

-- ============================================================
-- 11. Three-column: html + markdown + plain
-- ============================================================

DROP TABLE IF EXISTS markup_3c_hmp CASCADE;
CREATE TABLE markup_3c_hmp (
  id int PRIMARY KEY,
  col_html text,
  col_md text,
  col_plain text
);
INSERT INTO markup_3c_hmp VALUES
  (1,
   '<div>HTML <a href="http://x.com">visible</a></div>',
   '# Heading with **emphasis**',
   'Just plain text here');
CREATE INDEX markup_3c_hmp_idx ON markup_3c_hmp
  USING bm25(col_html, col_md, col_plain)
  WITH (text_config = 'english',
        content_format = 'col_html:html,col_md:markdown,col_plain:plain');

-- HTML column
SELECT id FROM markup_3c_hmp
  ORDER BY (col_html, col_md, col_plain)
       <@> to_bm25query('visible', 'markup_3c_hmp_idx') LIMIT 1;

-- HTML href NOT searchable
SELECT id,
  (col_html, col_md, col_plain)
    <@> to_bm25query('x.com', 'markup_3c_hmp_idx') AS score
  FROM markup_3c_hmp WHERE id = 1;

-- Markdown column
SELECT id FROM markup_3c_hmp
  ORDER BY (col_html, col_md, col_plain)
       <@> to_bm25query('emphasis', 'markup_3c_hmp_idx') LIMIT 1;

-- Plain column
SELECT id FROM markup_3c_hmp
  ORDER BY (col_html, col_md, col_plain)
       <@> to_bm25query('plain', 'markup_3c_hmp_idx') LIMIT 1;

-- ============================================================
-- 12. Three-column: all same format (bare markdown)
-- ============================================================

DROP TABLE IF EXISTS markup_3c_bare CASCADE;
CREATE TABLE markup_3c_bare (
  id int PRIMARY KEY, a text, b text, c text
);
INSERT INTO markup_3c_bare VALUES
  (1, '# Title A', '**Bold B**', '- Item C');
CREATE INDEX markup_3c_bare_idx ON markup_3c_bare USING bm25(a, b, c)
  WITH (text_config = 'english', content_format = 'markdown');

SELECT id FROM markup_3c_bare
  ORDER BY (a, b, c) <@> to_bm25query('title', 'markup_3c_bare_idx') LIMIT 1;

SELECT id FROM markup_3c_bare
  ORDER BY (a, b, c) <@> to_bm25query('bold', 'markup_3c_bare_idx') LIMIT 1;

SELECT id FROM markup_3c_bare
  ORDER BY (a, b, c) <@> to_bm25query('item', 'markup_3c_bare_idx') LIMIT 1;

-- ============================================================
-- 13. Three-column: all same format (bare html)
-- ============================================================

DROP TABLE IF EXISTS markup_3c_bare_html CASCADE;
CREATE TABLE markup_3c_bare_html (
  id int PRIMARY KEY, a text, b text, c text
);
INSERT INTO markup_3c_bare_html VALUES
  (1, '<b>Bold A</b>', '<p>Para B &amp; more</p>', '<em>Italic C</em>');
CREATE INDEX markup_3c_bh_idx ON markup_3c_bare_html USING bm25(a, b, c)
  WITH (text_config = 'english', content_format = 'html');

SELECT id FROM markup_3c_bare_html
  ORDER BY (a, b, c) <@> to_bm25query('bold', 'markup_3c_bh_idx') LIMIT 1;

SELECT id FROM markup_3c_bare_html
  ORDER BY (a, b, c) <@> to_bm25query('para', 'markup_3c_bh_idx') LIMIT 1;

SELECT id FROM markup_3c_bare_html
  ORDER BY (a, b, c) <@> to_bm25query('italic', 'markup_3c_bh_idx') LIMIT 1;

-- Tags not searchable
SELECT id, (a, b, c) <@> to_bm25query('em', 'markup_3c_bh_idx') AS score
  FROM markup_3c_bare_html WHERE id = 1;

-- ============================================================
-- 14. INSERT into multi-column normalized index
-- ============================================================

INSERT INTO markup_3c VALUES
  (10,
   'Inserted Plain',
   '<p>Inserted <b>HTML</b></p>',
   '**Inserted** markdown');

SELECT id FROM markup_3c
  ORDER BY (title, summary, body) <@> to_bm25query('insert', 'markup_3c_idx')
  LIMIT 1;

-- ============================================================
-- 15. Snippets: single-column plain (baseline)
-- ============================================================

SELECT bm25_snippet(
  content,
  to_bm25query('hello', 'markup_plain_idx'))
FROM markup_plain WHERE id = 1;

-- ============================================================
-- 16. Snippets: single-column HTML
-- ============================================================

-- Visible text highlighted, tags stripped in snippet
SELECT bm25_snippet(
  content,
  to_bm25query('friend', 'markup_html_idx'))
FROM markup_html WHERE id = 1;

-- Script content stripped, safe text shown
SELECT bm25_snippet(
  content,
  to_bm25query('safe', 'markup_html_idx'))
FROM markup_html WHERE id = 3;

-- Entity decoded in snippet
SELECT bm25_snippet(
  content,
  to_bm25query('tom', 'markup_html_idx'))
FROM markup_html WHERE id = 4;

-- ============================================================
-- 17. Snippets: single-column Markdown
-- ============================================================

-- Bold syntax stripped
SELECT bm25_snippet(
  content,
  to_bm25query('bold', 'markup_md_idx'))
FROM markup_md WHERE id = 1;

-- Link label highlighted, URL stripped
SELECT bm25_snippet(
  content,
  to_bm25query('click', 'markup_md_idx'))
FROM markup_md WHERE id = 2;

-- Code text highlighted
SELECT bm25_snippet(
  content,
  to_bm25query('hello', 'markup_md_idx'))
FROM markup_md WHERE id = 3;

-- List items highlighted
SELECT bm25_snippet(
  content,
  to_bm25query('item', 'markup_md_idx'))
FROM markup_md WHERE id = 4;

-- ============================================================
-- 18. Snippet positions on normalized content
-- ============================================================

-- HTML: positions are against original text
SELECT bm25_snippet_positions(
  content,
  to_bm25query('friend', 'markup_html_idx'))
FROM markup_html WHERE id = 1;

-- Markdown: positions against original text
SELECT bm25_snippet_positions(
  content,
  to_bm25query('bold', 'markup_md_idx'))
FROM markup_md WHERE id = 1;

-- ============================================================
-- 19. Snippets: two-column with per-field format
-- ============================================================

-- plain title + html body: snippet per field
SELECT bm25_snippet(
  title,
  to_bm25query('plain', 'markup_multi_idx'),
  field_name => 'title') AS title_snippet
FROM markup_multi WHERE id = 1;

SELECT bm25_snippet(
  body,
  to_bm25query('body', 'markup_multi_idx'),
  field_name => 'body') AS body_snippet
FROM markup_multi WHERE id = 1;

-- plain title + markdown body
SELECT bm25_snippet(
  title,
  to_bm25query('plain', 'markup_2c_pm_idx'),
  field_name => 'title') AS title_snippet
FROM markup_2c_pm WHERE id = 1;

SELECT bm25_snippet(
  body,
  to_bm25query('bold', 'markup_2c_pm_idx'),
  field_name => 'body') AS body_snippet
FROM markup_2c_pm WHERE id = 1;

-- html title + markdown body
SELECT bm25_snippet(
  title,
  to_bm25query('html', 'markup_2c_hm_idx'),
  field_name => 'title') AS title_snippet
FROM markup_2c_hm WHERE id = 1;

SELECT bm25_snippet(
  body,
  to_bm25query('bold', 'markup_2c_hm_idx'),
  field_name => 'body') AS body_snippet
FROM markup_2c_hm WHERE id = 1;

-- ============================================================
-- 20. Snippets: three-column mixed format
-- ============================================================

-- plain + html + markdown: snippet each field
SELECT bm25_snippet(
  title,
  to_bm25query('plain', 'markup_3c_idx'),
  field_name => 'title') AS title_snippet
FROM markup_3c WHERE id = 1;

SELECT bm25_snippet(
  summary,
  to_bm25query('bold', 'markup_3c_idx'),
  field_name => 'summary') AS summary_snippet
FROM markup_3c WHERE id = 1;

SELECT bm25_snippet(
  body,
  to_bm25query('format', 'markup_3c_idx'),
  field_name => 'body') AS body_snippet
FROM markup_3c WHERE id = 1;

-- ============================================================
-- 21. bm25_headline JSON on normalized multi-column
-- ============================================================

SELECT bm25_headline(
  to_bm25query('emphasis', 'markup_3c_hmp_idx'),
  'markup_3c_hmp_idx',
  VARIADIC ARRAY[col_html, col_md, col_plain])
FROM markup_3c_hmp WHERE id = 1;

-- ============================================================
-- 22. INSERT into single-column normalized index
-- ============================================================

INSERT INTO markup_html VALUES
  (10, '<p>Newly <em>inserted</em> document</p>');

SELECT id FROM markup_html
  ORDER BY content <@> to_bm25query('insert', 'markup_html_idx') LIMIT 1;

-- ============================================================
-- 23. Invalid format errors
-- ============================================================

DROP TABLE IF EXISTS markup_err CASCADE;
CREATE TABLE markup_err (id int PRIMARY KEY, content text);

\set ON_ERROR_ROLLBACK on
\set ON_ERROR_STOP off

CREATE INDEX markup_err_idx ON markup_err USING bm25(content)
  WITH (text_config = 'english', content_format = 'xml');

\set ON_ERROR_STOP on

-- ============================================================
-- Cleanup
-- ============================================================

DROP TABLE IF EXISTS markup_plain CASCADE;
DROP TABLE IF EXISTS markup_html CASCADE;
DROP TABLE IF EXISTS markup_md CASCADE;
DROP TABLE IF EXISTS markup_multi CASCADE;
DROP TABLE IF EXISTS markup_bare CASCADE;
DROP TABLE IF EXISTS markup_2c_pm CASCADE;
DROP TABLE IF EXISTS markup_2c_hm CASCADE;
DROP TABLE IF EXISTS markup_2c_mh CASCADE;
DROP TABLE IF EXISTS markup_2c_bh CASCADE;
DROP TABLE IF EXISTS markup_3c CASCADE;
DROP TABLE IF EXISTS markup_3c_hmp CASCADE;
DROP TABLE IF EXISTS markup_3c_bare CASCADE;
DROP TABLE IF EXISTS markup_3c_bare_html CASCADE;
DROP TABLE IF EXISTS markup_err CASCADE;
DROP EXTENSION pg_textsearch CASCADE;
