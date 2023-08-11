# Post-processor
Handling post processing for result from domain or twitter crawlers
- the post-processor scripts are under `/processor/`
- the old back-end scripts are in `/archived/`
- `/utils/` are for scripts to prepare data for postprocessing:
- - header cleaner removes unnecessary headers from crawler output csv files from Twitter crawler
  - metascraper This tool can be used to populate the title_metascraper, author_metascraper, date, html_content, article_text fields of JSONs produced by the domain crawler.
  - urlexpander lengthens short urls in tweets  

