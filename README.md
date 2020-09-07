# Sentiment-Analysis-for-Tweets-from-Top-500-Companies-Under-COVID-19-Situation

This is an ongoing project analyzing tweets from top 500 companies from late January to early July, in order to study how the companies reacted to COVID-19 and how the public (including customers, stockholders, investors, and employees) replied to their tweets. Particularly in March, businesses in the U.S. shut down because of the policy. Therefore, we intend to dig out changes of both the top 500 companies' and the public's sentiments between periods before and after the execution of shutdown orders.

The first step is data scraping to gather tweets relevant to COVID-19 from the top 500 companies along with their comments. (https://github.com/MeixianWang2/Twitter-Scraping-comments/tree/master/get_reply)

Then we are able to conduct sentiment analysis to the data we collected.

In the naive-bayes-classifier script I built a Naive Bayes Classification model to detect the overall sentiment polarity of the entire tweets data at a high level. (Source: https://towardsdatascience.com/creating-the-twitter-sentiment-analysis-program-in-python-with-naive-bayes-classification-672e5589a7ed)
