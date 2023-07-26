const fs = require('fs');
const csvParser = require('csv-parser');
const childProcess = require('child_process');
const getMetadata = require('../../mediacat-domain-crawler/newCrawler/getDates');
require('dotenv').config();

const PATH_SCOPE_PARSER = process.env.COMMANDLINE_PATH_SCOPE_PARSER || '../../mediacat-frontend/scope_parser/main.py';
const PATH_INPUT_CSV = process.env.COMMANDLINE_PATH_INPUT_CSV || '../../mediacat-frontend/scope_parser/csv/test_demo.csv';

const PATH_TWITTER_CRAWLER= process.env.COMMANDLINE_PATH_TWITTER_CRAWLER || '../../mediacat-twitter-crawler/main.py';
const PATH_DOMAIN_CRAWLER= process.env.COMMANDLINE_PATH_DOMAIN_CRAWLER || '../../mediacat-domain-crawler/newCrawler/crawl.js';

const FAILED_DOMAIN_LINKS= process.env.COMMANDLINE_FAILED_DOMAIN_LINKS || './failed_links_list.json';
const VALID_DOMAIN_LINKS= process.env.COMMANDLINE_VALID_DOMAIN_LINKS || './link_title_list.json';

const domaincsvFile = process.env.COMMANDLINE_domaincsvFile || './domain.csv';
const twittercsvFile = process.env.COMMANDLINE_twittercsvFile || './twitter.csv';

const metadataJSON = process.env.COMMANDLINE_metadataJSON || './metadata_modified_list.json';

const start_date = process.env.COMMANDLINE_TWT_START_DATE || null;
const end_date = process.env.COMMANDLINE_TWT_END_DATE || null;
const keyword = process.env.COMMANDLINE_TWT_KEYWORD || null;

/**
 * checks for the correct number of arguments and calles the appropriate function
 * `node app.js twitter ` to run the twitter crawler
 * `set in .env start_date end_date` to run the twitter crawler with dates
 * `set in .env start_date end_date keyword` to run the twitter crawler with dates and a keyword
 * `node app.js domain ` to run the domain crawler
 * `node app.js` to run everything
 * dates in YYYY-MM-DD form
 */

function run() {
  var myArgs = process.argv.slice(2);    
  switch (myArgs[0]) {
  case 'twitter':
    console.log('twitter crawler initiated...');
    appTwitter();
    break;
  case 'domain':
    console.log('domain crawler initiated...');
    appDomain();
    break;
  default:
    console.log('twitter and domain crawler initiated...');
    app();
  }
}

/**
 * calls the scope parser, then the two crawlers
 */
function app() {
  stepOne(stepTwo);
}

/**
 * calls the scope parser, then the twitter crawler
 */
function appTwitter() {
  stepOne(stepTwoTwitter);
}

/**
 * calls the scope parser, then the app crawler
 */
function appDomain() {
  stepOne(stepTwoDomain);
}

/**************************************************/

function stepOne(next) {
  /**
   * Step one: feed in the input files into scope parser
   */

  try {
    const pythonProcess = childProcess.spawn('python3',[PATH_SCOPE_PARSER, PATH_INPUT_CSV], {
      shell: true
    });

    pythonProcess.on('close', () => {
      next();
    });
  
    pythonProcess.stderr.on('data', function (data) {
      console.error('STDERR:', data.toString());
      process.exit(1);
    });

    pythonProcess.stdout.on('data', function (data) {
      console.log('STDOUT:', data.toString());
    });

    pythonProcess.on('exit', function (exitCode, signalCode) {
      if (exitCode) {
        console.error('Child exited with code', exitCode);
      } else if (signalCode) {
        console.error('Child was killed with signal', signalCode);
      } else {
        console.log('Child exited with code: ' + exitCode);
      }
    });

  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

/**
 * Step Two: feed to the crawlers
 */

/**
 * helper function that calls the two crawlers
 * is used by stepOne
 */

function stepTwo() {
  // twitter crawler
  stepTwoTwitter();

  // domain crawler
  stepTwoDomain();
}

/*******************Twitter*******************************/

/**
 * calls the twitter crawler
 */

function stepTwoTwitter() {
  try {
    if (fs.existsSync(twittercsvFile)) {
      console.log('File %s exists!', twittercsvFile);
      //file exists
    }
  } catch(err) {
    console.error(err);
    process.exit(1);
  }

  /**
   * Twitter crawler:
   * install twint on the correct branch version
   * : pip3 install --user --upgrade git+https://github.com/twintproject/twint.git@origin/master#egg=twint
   */
  
  try {

    let pythonProcess2 = childProcess.spawn( `python3 ${PATH_TWITTER_CRAWLER} ${twittercsvFile}`, {
      shell: true
    });

    if (start_date !== null && end_date != null && keyword != null ) {
      pythonProcess2 = childProcess.spawn( `python3 ${PATH_TWITTER_CRAWLER} ${twittercsvFile} ${start_date} ${end_date} ${keyword}`, {
        shell: true
      });

    } else if (start_date !== null && end_date != null ) {
      pythonProcess2 = childProcess.spawn( `python3 ${PATH_TWITTER_CRAWLER} ${twittercsvFile} ${start_date} ${end_date}`, {
        shell: true
      });
    } 

    pythonProcess2.on('close', () => {
      callbackAfterTwitterCrawler();
    });
  
    pythonProcess2.stderr.on('data', function (data) {
      console.error('STDERR:', data.toString());
    });

    pythonProcess2.stdout.on('data', function (data) {
      console.log('STDOUT:', data.toString());
    });

    pythonProcess2.on('exit', function (exitCode) {
      console.log('Child exited with code: ' + exitCode);
    });

  } catch (err) {
    console.error(err);
  }
}

function callbackAfterTwitterCrawler() {
  console.log('finished crawling twitter');
  console.log('all twitter csvs should be ready');

}

/*******************Twitter*******************************/

/*******************Domain*******************************/

function stepTwoDomain() {

  try {
    if (fs.existsSync(domaincsvFile)) {
      console.log('File %s exists!', domaincsvFile);
    }
  } catch(err) {
    console.error(err);
    process.exit(1);
  }

  readDomainInput(domaincsvFile, callDomainCrawler, callbackAfterDomainCrawler);
}

/**
 * Read csv of domains and call the domain crawler
 * @param {*} file 
 */
function readDomainInput(file, callback, callback2) {
  const results = [];
  try {
    fs.createReadStream(file)
      .pipe(csvParser())
      .on('data', (data) => results.push(data))
      .on('end', () => {
        return (callback(results, callback2));
      });
  } catch (err) {
    console.log(err);
  }
}

/**
 * calls the domain crawler
 * feed the urls to the domain crawler,
 * make sure to `npm install`
 * @param {*} domainlist 
 */

function callDomainCrawler(domainlist, callback) {
  //let crawler = require(PATH_DOMAIN_CRAWLER)
  let i;
  let list = ['-l'];
  for (i = 0; i < domainlist.length; i++) {
    let domain = domainlist[i];
    let source = domain.Source;
    list.push(source);
  }

  console.log(list);
  const command = PATH_DOMAIN_CRAWLER;

  try {
    const domainCrawlProcess = childProcess.fork(command, list);

    // listen for errors as they may prevent the exit event from firing
    domainCrawlProcess.on('error', function (err) {
      console.log(err);
    });

    // execute the callback once the process has finished running
    domainCrawlProcess.on('exit', function (code) {
      var err = code === 0 ? null : new Error('exit code ' + code);
      if (err) {
        console.log(err);
      }
      callback();
    });
  } catch (err) {
    console.log(err);
  }

}

function callbackAfterDomainCrawler() {
  console.log('finished crawling domains');
  console.log('all domain jsons should be ready');
  console.log('domain crawl is complete');

  try {
    if (fs.existsSync(FAILED_DOMAIN_LINKS)) {
      console.log('File %s exists!', FAILED_DOMAIN_LINKS);
    }
  } catch(err) {
    console.error(err);
  }

  try {
    if (fs.existsSync(VALID_DOMAIN_LINKS)) {
      console.log('File %s exists!', VALID_DOMAIN_LINKS);
      //file exists
    }
  } catch(err) {
    console.error(err);
  }

  callToMetascraper();

}

/*******************Domain*******************************/
/*******************Post*******************************/


/**
 * Take the output of the crawlers and 
 * stuff it into date processor
 */

async function callToMetascraper(){
  await getMetadata.getDate(VALID_DOMAIN_LINKS);
  console.log('metascraping is complete!');

  try {
    if (fs.existsSync(metadataJSON)) {
      console.log('File %s exists!', metadataJSON);
    }
    callToPostProcessing();
  } catch(err) {
    console.error(err);
  }

}


/**
 * Step Four: Take the output of the date processor and 
 * stuff it into post processor
 */



/**
 * Call back to post processing after crawler is done
 * 
 */
function callToPostProcessing(){
  console.log('call to post processing...');
}

/*******************Post*******************************/

/**
 * uncomment commands as needed
 */
if (require.main === module) {
  run();
  // app();
  // appDomain();
  // appTwitter();
}
