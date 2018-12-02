
Episode1:
SELECT created_utc,body,score,author FROM [fh-bigquery:reddit_comments.2017_07] where  subreddit="gameofthrones" and created_utc between 1500253200  and 1500858000 and length(body) > 10 and body not like "%removed%" and body not like "%http%" and body not like"%spoiler%" 
order by created_utc

Episode2:
SELECT created_utc,body,score,author FROM [fh-bigquery:reddit_comments.2017_07] where  subreddit="gameofthrones" and created_utc between 1500858000  and 1501462800 and length(body) > 10 and body not like "%removed%" and body not like "%http%" and body not like"%spoiler%" 
order by created_utc

Episode3:
SELECT created_utc,body,score,author FROM [fh-bigquery:reddit_comments.2017_07] where  subreddit="gameofthrones" and created_utc between 1501462800  and 1501545594 and length(body) > 10 and body not like "%removed%" and body not like "%http%" and body not like"%spoiler%" 
order by created_utc

SELECT created_utc,body,score,author FROM [fh-bigquery:reddit_comments.2017_08] where  subreddit="gameofthrones" and created_utc between 1501545594  and 1502067600 and length(body) > 10 and body not like "%removed%" and body not like "%http%" and body not like"%spoiler%" 
order by created_utc

Episode4:
SELECT created_utc,body,score,author FROM [fh-bigquery:reddit_comments.2017_08] where  subreddit="gameofthrones" and created_utc between 1502067600  and 1502672400 and length(body) > 10 and body not like "%removed%" and body not like "%http%" and body not like"%spoiler%" 
order by created_utc

Episode5:
SELECT created_utc,body,score,author FROM [fh-bigquery:reddit_comments.2017_08] where  subreddit="gameofthrones" and created_utc between 1502672400  and 1503277200 and length(body) > 10 and body not like "%removed%" and body not like "%http%" and body not like"%spoiler%" 
order by created_utc

Episode6:
SELECT created_utc,body,score,author FROM [fh-bigquery:reddit_comments.2017_08] where  subreddit="gameofthrones" and created_utc between 1503277200  and 1503882000 and length(body) > 10 and body not like "%removed%" and body not like "%http%" and body not like"%spoiler%" 
order by created_utc

Episode7:

SELECT created_utc,body,score,author FROM [fh-bigquery:reddit_comments.2017_08] where  subreddit="gameofthrones" and created_utc between 1503882000  and 1504224012 and length(body) > 10 and body not like "%removed%" and body not like "%http%" and body not like"%spoiler%" 
order by created_utc

SELECT created_utc,body,score,author FROM [fh-bigquery:reddit_comments.2017_09] where  subreddit="gameofthrones" and created_utc between 1504224012  and 1504486800 and length(body) > 10 and body not like "%removed%" and body not like "%http%" and body not like"%spoiler%" 
order by created_utc