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



---------Working with Git ------------------

1. **********store your file that you have in localdirectory into a backupfolder since when you clone the git remote repository, you might lose the file  ( IMPORTANt MUST DO)******
2. git clone https://github.com/kirusanth/EECS4415-BigDataProject.git
2. cd EECS4415-BigDataProject
3. git fetch
4. git checkout <yourbranch name>  ------> task-daniel, task-kristin,task-kirusanth, task-luke, 
5. put the files you work in that directory 
5. git add .
6. git commit -m "Type your message for the commit"
7. git push origin <yourbranch name>
8. go to the git hub and to your branch, then click on create new pull request button ---> this will point to master and create request to update changes into master branch which i can add after reviewing with you guys



if you want to check whether the file is added or not:
you can use following command:

git status



