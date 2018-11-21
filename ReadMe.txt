#compose google query urls given a file where query keywords are listed each one line.
    #-num integer is used to set how many result entries are returned on each page.
    python3 ./composeQueries.py -qf keywordFile -num 30 ./resultQueryFile
    #you can use -h to show help document
    python3 ./composeQueries.py -h


#Once you have finished compose google query urls, you can start to query
    #This is a typical example where the crawling will run  in 10 processes with each containing 30 threads.
    python3 ./crawlGoogle.py ./queryFile ./resutlDir -th 30 -p 10  
    #Of course, you can use -h to show help documentation
    python3 ./crawlGoogle.py -h

#to crawl google using multiple servers, such as servers listed in availableHostList.txt, conduct the following steps:
#
0. you may need to install pexpect python module by running python -m pip install --user pexpect
1. create a dir exeDir and copy crawlGoogle.py and logUtil.py to exeDir
2. python3 ./crawlRelay.py search exeDir resultDir ./availableHostList.txt -du xmi -dp passwordToLoginRemoteServer 

#Once the crawling finished, you can check the resultDir, there must be an sub directory named "entrypages" where stores the html files of the query resulting. Mapping between each result file and the corresponding query url is stored in "UrlQueryResultList" in json format.  A typical mapping is shown as below. The contentHash field is used to name the result html file.
#One thing to mention, since google search has request limitation, when the limitation is satisfied, the crawling script will stop all the work, and output the unfinished query urls to retryList in resultDir, which can be used to start the next round once the query limitation expires several hours later.
    {
        "content-type": "text/html; charset=utf-8",
        "contentHash": "c127fcd5f0b969cf19210567df44439f",
        "requestUrl": "https://www.google.com/search?hl=en&num=20&start=0&q=advisor directed trust franklin templeton",
        "requestUrlHash": "c127fcd5f0b969cf19210567df44439f",
        "status": 200,
        "timeCost": 0.0023593902587890625,
        "url": "https://www.google.com/search?hl=en&num=20&start=0&q=advisor%20directed%20trust%20franklin%20templeton"
    }

#Once you finish crawling all the query urls, you can start to parse them. -p will set the worker processes.
    python3 ./parseGoogleSearchMultiProc.py -p 15 ./googleQueryResultDir ./googleParseResultDir

#Once the parsing is finished, "googlePageParseResult.json" file will show in the "googleParseResultDir" 
#Each line is a json object, among which, "entities" field is the list of entries in the result google query page.
