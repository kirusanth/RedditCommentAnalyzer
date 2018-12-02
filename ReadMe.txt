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
3. git fetch *IMPORTANT ONLY ONE TIME to get all the folders in master* (ONLY FIRST TIME - This command copy all the files into your directory deleting the files that is not in the branch) / if you want to fetch from branch, switch to branch then fetch ( do step 4 before step 3)
4. git checkout <yourbranch name>  ------> task-daniel, task-kristin,task-kirusanth, task-luke
5. put the files you work in that directory 
5. git add .
6. git commit -m "Type your message for the commit"
7. git push origin <yourbranch name>
8. YOU CAN SEE THE FILES IN YOUR BRANCH IN GITHUB.com 
9. click on the Pull request on the navbar
10. Then click on create new pull request and set the base:master and compare: <yourbranch name> , it will compare master with your branch so you can see the difference
11. Click on create pull request

--if you want to add more files or you update the content and now you want to add it your branch---
Then you use following commands since you already checkout the branch:
1.git add .
2.git commit -m "yourmessage"
3.git push origin <yourbranchname>



***DO NOT CHECKOUT MASTER branch , please work on your branch then create pull requests on github , so i cann add the changes to master*********** 





if you want to check whether the file is added or not:
you can use following command:

git status





