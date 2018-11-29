1. Open powershell
2. go to RedditCommentProjectfolder ( contains Gameofthrones folder and src)
3. run eecsyorku docker image from the location:
	 docker run -it -v ${PWD}:/app eecsyorku/eecs4415
4. run following commands on docker:
	hadoop fs -mkdir -p /user
	hadoop fs -put GameofThrones /user
5. go to src in the app to run sentimentCharacter.py file as following:
	spark-submit SentimentCharacters.py

6. If you see the file rows in the terminal means you are ready to go

