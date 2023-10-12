# Post-processor
Handling post processing for results from domain or twitter crawlers
- the post-processor scripts are under `processor/`
- the old back-end scripts are in `archived/`
- `scripts/` are handy automation scripts to prepare data for postprocessing:
  - cleaner removes unnecessary headers and duplicate records from twitter crawler output csv files 
  - metascraper can be used to populate the title\_metascraper, author\_metascraper, date, html\_content, and article\_text JSON fields produced by the domain crawler
  - url\_expander lengthens short urls in tweets  

